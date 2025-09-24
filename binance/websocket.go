package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// SystemState WebSocket系统状态
type SystemState string

const (
	SystemStateNormal     SystemState = "NORMAL"
	SystemStateDegraded   SystemState = "DEGRADED"
	SystemStateRecovering SystemState = "RECOVERING"
)

// WSClient WebSocket客户端
type WSClient struct {
	baseURL           string
	conn              *websocket.Conn
	mu                sync.RWMutex
	isConnected       bool
	handlers          map[string]func([]byte)
	ctx               context.Context
	cancel            context.CancelFunc
	reconnectCh       chan struct{}
	pingTicker        *time.Ticker
	
	// 心跳机制
	lastTransportFrame time.Time  // 传输心跳：最后收到任何帧的时间
	lastBusinessFrame  time.Time  // 业务心跳：最后收到业务事件的时间
	systemState        SystemState
	stateMu            sync.RWMutex
	
	// 重连配置
	maxRetries        int
	retryInterval     time.Duration
	currentRetries    int
	
	// listenKey管理
	listenKey         string
	listenKeyExpiry   time.Time
	listenKeyCreated  time.Time  // 新增：记录listenKey创建时间
	keepAliveTicker   *time.Ticker
	
	// 回调函数
	onStateChange     func(SystemState)
	onOrderUpdate     func(*OrderUpdateEvent)
	onAccountUpdate   func(*AccountUpdateEvent)
	onMarkPrice       func(*MarkPriceEvent)
	
	// 外部组件引用
	degradedMode      *DegradedMode
	recoveryManager   interface{} // 避免循环依赖，使用interface{}
	binanceClient     interface{} // 币安客户端引用，用于keepalive
	reducer           interface{} // Reducer引用，用于监控记录
	
	// 单writer模式：避免并发写入
	writeChan         chan writeMessage
	writerDone        chan struct{}
}

// writeMessage 定义写入消息的结构
type writeMessage struct {
	messageType int
	data        []byte
	jsonData    interface{}
	responseChan chan error
}

// NewWSClient 创建WebSocket客户端
func NewWSClient(baseURL string) *WSClient {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &WSClient{
		baseURL:         baseURL,
		handlers:        make(map[string]func([]byte)),
		ctx:             ctx,
		cancel:          cancel,
		reconnectCh:     make(chan struct{}, 1),
		maxRetries:      10,
		retryInterval:   5 * time.Second,
		systemState:     SystemStateNormal,
		lastTransportFrame: time.Now(),
		lastBusinessFrame:  time.Now(),
		writeChan:       make(chan writeMessage, 100), // 缓冲通道
		writerDone:      make(chan struct{}),
	}
}

// SetExternalComponents 设置外部组件
func (ws *WSClient) SetExternalComponents(degradedMode *DegradedMode, recoveryManager interface{}) {
	ws.degradedMode = degradedMode
	ws.recoveryManager = recoveryManager
}

// SetReducer 设置Reducer
func (ws *WSClient) SetReducer(reducer interface{}) {
	ws.reducer = reducer
}

// SetBinanceClient 设置币安客户端
func (ws *WSClient) SetBinanceClient(client interface{}) {
	ws.binanceClient = client
}

// SetCallbacks 设置回调函数
func (ws *WSClient) SetCallbacks(
	onStateChange func(SystemState),
	onOrderUpdate func(*OrderUpdateEvent),
	onAccountUpdate func(*AccountUpdateEvent),
	onMarkPrice func(*MarkPriceEvent),
) {
	ws.onStateChange = onStateChange
	ws.onOrderUpdate = onOrderUpdate
	ws.onAccountUpdate = onAccountUpdate
	ws.onMarkPrice = onMarkPrice
}

// SetListenKey 设置listenKey
func (ws *WSClient) SetListenKey(listenKey string, expiryMinutes int) {
	ws.listenKey = listenKey
	ws.listenKeyCreated = time.Now()
	ws.listenKeyExpiry = time.Now().Add(time.Duration(expiryMinutes) * time.Minute)
}

// GetSystemState 获取系统状态
func (ws *WSClient) GetSystemState() SystemState {
	ws.stateMu.RLock()
	defer ws.stateMu.RUnlock()
	return ws.systemState
}

// setSystemState 设置系统状态
func (ws *WSClient) setSystemState(state SystemState) {
	ws.stateMu.Lock()
	defer ws.stateMu.Unlock()
	
	if ws.systemState != state {
		oldState := ws.systemState
		ws.systemState = state
		log.Printf("WebSocket state changed: %s -> %s", oldState, state)
		
		if ws.onStateChange != nil {
			ws.onStateChange(state)
		}
		
		// 根据状态变化触发相应的操作
		switch state {
		case SystemStateDegraded:
			if ws.degradedMode != nil {
				go ws.degradedMode.Start()
			}
		case SystemStateRecovering:
			// 恢复状态由RecoveryManager管理
		case SystemStateNormal:
			if ws.degradedMode != nil && ws.degradedMode.IsActive() {
				go ws.degradedMode.Stop()
			}
		}
	}
}

// Connect 连接WebSocket
func (ws *WSClient) Connect(endpoint string) error {
	u, err := url.Parse(ws.baseURL + endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse WebSocket URL: %w", err)
	}

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	ws.mu.Lock()
	ws.conn = conn
	ws.isConnected = true
	ws.currentRetries = 0
	ws.lastTransportFrame = time.Now()
	ws.lastBusinessFrame = time.Now()
	ws.mu.Unlock()

	// 设置为正常状态
	ws.setSystemState(SystemStateNormal)

	// 启动单writer goroutine
	go ws.startWriter()

	// 启动心跳监控
	ws.startHeartbeatMonitor()
	
	// 启动keepalive（如果有listenKey）
	if ws.listenKey != "" {
		ws.startKeepAlive(ws.binanceClient)
	}

	// 启动消息处理
	go ws.handleMessages()

	log.Printf("WebSocket connected to %s", u.String())
	return nil
}

// ConnectUserDataStream 连接用户数据流
func (ws *WSClient) ConnectUserDataStream(listenKey string) error {
	ws.SetListenKey(listenKey, 60) // listenKey有效期60分钟
	endpoint := fmt.Sprintf("/ws/%s", listenKey)
	return ws.Connect(endpoint)
}

// startHeartbeatMonitor 启动心跳监控
func (ws *WSClient) startHeartbeatMonitor() {
	go func() {
		ticker := time.NewTicker(5 * time.Second) // 每5秒检查一次
		defer ticker.Stop()
		
		for {
			select {
			case <-ws.ctx.Done():
				return
			case <-ticker.C:
				ws.checkHeartbeat()
			}
		}
	}()
}

// checkHeartbeat 检查心跳状态
func (ws *WSClient) checkHeartbeat() {
	ws.mu.RLock()
	lastFrame := ws.lastTransportFrame
	listenKeyCreated := ws.listenKeyCreated
	ws.mu.RUnlock()
	
	// 检查传输心跳：超过60秒没有任何帧（增加超时时间）
	if time.Since(lastFrame) > 60*time.Second {
		log.Printf("Transport heartbeat timeout: %v since last frame", time.Since(lastFrame))
		ws.setSystemState(SystemStateDegraded)
		ws.triggerReconnect()
		return
	}
	
	// 如果超过30秒没有收到消息，记录警告但不重连
	if time.Since(lastFrame) > 30*time.Second {
		log.Printf("Transport heartbeat warning: %v since last frame", time.Since(lastFrame))
	}
	
	// 检查listenKey是否需要强制旋转（23小时50分钟）
	if !listenKeyCreated.IsZero() && time.Since(listenKeyCreated) >= 23*time.Hour+50*time.Minute {
		log.Printf("ListenKey approaching expiry (%v old), triggering smooth rotation", time.Since(listenKeyCreated))
		ws.triggerSmoothRotation()
	}
}

// triggerSmoothRotation 触发平滑旋转
func (ws *WSClient) triggerSmoothRotation() {
	log.Println("Starting smooth listenKey rotation...")
	
	// 设置为恢复状态，避免触发降级模式
	ws.setSystemState(SystemStateRecovering)
	
	// 触发重连，这将会获取新的listenKey
	select {
	case ws.reconnectCh <- struct{}{}:
		log.Println("Smooth rotation reconnect triggered")
	default:
		log.Println("Reconnect already in progress")
	}
}

// startKeepAlive 启动listenKey保活
func (ws *WSClient) startKeepAlive(client interface{}) {
	if ws.keepAliveTicker != nil {
		ws.keepAliveTicker.Stop()
	}
	
	// 每25分钟发送一次keepalive（比30分钟提前一些）
	ws.keepAliveTicker = time.NewTicker(25 * time.Minute)
	
	go func() {
		defer ws.keepAliveTicker.Stop()
		
		for {
			select {
			case <-ws.ctx.Done():
				return
			case <-ws.keepAliveTicker.C:
				ws.mu.RLock()
				lk := ws.listenKey
				ws.mu.RUnlock()
				
				if lk == "" {
					log.Printf("No listenKey to keep alive")
					continue
				}
				
				// 调用REST API延长listenKey有效期
				if binanceClient, ok := client.(interface{ KeepAliveListenKey(string) error }); ok {
					if err := binanceClient.KeepAliveListenKey(lk); err != nil {
						log.Printf("ListenKey keepalive failed: %v, triggering recovery", err)
						// 保活失败时立即切换到恢复状态并触发重连
						ws.setSystemState(SystemStateRecovering)
						ws.triggerReconnect()
						continue
					}
					log.Printf("ListenKey keepalive successful for key: %s", lk[:8]+"...")
					// 更新listenKey过期时间
					ws.mu.Lock()
					ws.listenKeyExpiry = time.Now().Add(60 * time.Minute)
					ws.mu.Unlock()
				} else {
					log.Printf("Client does not support KeepAliveListenKey")
				}
			}
		}
	}()
}

// ConnectMarketStream 连接市场数据流
func (ws *WSClient) ConnectMarketStream(symbol string) error {
	symbol = strings.ToLower(symbol)
	endpoint := fmt.Sprintf("/ws/%s@markPrice", symbol)
	return ws.Connect(endpoint)
}

// Subscribe 订阅数据流
func (ws *WSClient) Subscribe(streams []string) error {
	msg := map[string]interface{}{
		"method": "SUBSCRIBE",
		"params": streams,
		"id":     time.Now().UnixNano(),
	}

	return ws.writeJSON(msg)
}

// Unsubscribe 取消订阅数据流
func (ws *WSClient) Unsubscribe(streams []string) error {
	msg := map[string]interface{}{
		"method": "UNSUBSCRIBE",
		"params": streams,
		"id":     time.Now().UnixNano(),
	}

	return ws.writeJSON(msg)
}

// SetHandler 设置消息处理器
func (ws *WSClient) SetHandler(eventType string, handler func([]byte)) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.handlers[eventType] = handler
}

// Close 关闭WebSocket连接
func (ws *WSClient) Close() error {
	ws.cancel()

	// 停止所有定时器
	if ws.pingTicker != nil {
		ws.pingTicker.Stop()
	}
	if ws.keepAliveTicker != nil {
		ws.keepAliveTicker.Stop()
	}

	// 等待writer goroutine结束
	select {
	case <-ws.writerDone:
	case <-time.After(1 * time.Second):
		log.Printf("Writer goroutine did not stop in time")
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.conn != nil {
		err := ws.conn.Close()
		ws.conn = nil
		ws.isConnected = false
		return err
	}

	return nil
}

// IsConnected 检查连接状态
func (ws *WSClient) IsConnected() bool {
	ws.mu.RLock()
	defer ws.mu.RUnlock()
	return ws.isConnected && ws.systemState == SystemStateNormal
}

// startPing 启动心跳
func (ws *WSClient) startPing() {
	ws.pingTicker = time.NewTicker(20 * time.Second)
	go func() {
		defer ws.pingTicker.Stop()
		for {
			select {
			case <-ws.ctx.Done():
				return
			case <-ws.pingTicker.C:
				if err := ws.writeMessage(websocket.PingMessage, nil); err != nil {
					log.Printf("Failed to send ping: %v", err)
					ws.triggerReconnect()
				}
			}
		}
	}()
}

// handleMessages 处理WebSocket消息
func (ws *WSClient) handleMessages() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("WebSocket message handler panic: %v", r)
		}
		ws.triggerReconnect()
	}()

	for {
		select {
		case <-ws.ctx.Done():
			return
		default:
			ws.mu.RLock()
			conn := ws.conn
			ws.mu.RUnlock()

			if conn == nil {
				return
			}

			// 设置读取超时
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("WebSocket read error: %v", err)
				ws.setSystemState(SystemStateDegraded)
				ws.triggerReconnect()
				return
			}

			// 更新传输心跳时间 - 任何帧都更新时间戳
			now := time.Now()
			ws.mu.Lock()
			ws.lastTransportFrame = now
			ws.mu.Unlock()

			// 处理不同类型的消息
			switch messageType {
			case websocket.TextMessage:
				ws.processMessage(message)
			case websocket.PongMessage:
				log.Printf("Received pong message")
				// Pong消息也更新业务心跳时间
				ws.mu.Lock()
				ws.lastBusinessFrame = now
				ws.mu.Unlock()
			case websocket.PingMessage:
				log.Printf("Received ping message")
				// Ping消息也更新业务心跳时间
				ws.mu.Lock()
				ws.lastBusinessFrame = now
				ws.mu.Unlock()
				// 自动回复pong
				if err := ws.writeMessage(websocket.PongMessage, nil); err != nil {
					log.Printf("Failed to send pong: %v", err)
				}
			case websocket.CloseMessage:
				log.Printf("Received close message")
				return
			}
		}
	}
}

// processMessage 处理具体消息内容
func (ws *WSClient) processMessage(message []byte) {
	var baseMsg map[string]interface{}
	if err := json.Unmarshal(message, &baseMsg); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return
	}

	// 检查是否是事件消息
	eventType, ok := baseMsg["e"].(string)
	if !ok {
		// 可能是订阅确认或其他系统消息
		log.Printf("Received system message: %s", string(message))
		return
	}

	// 更新业务心跳时间
	ws.mu.Lock()
	ws.lastBusinessFrame = time.Now()
	ws.mu.Unlock()

	// 处理不同类型的事件
	switch eventType {
	case "ORDER_TRADE_UPDATE":
		ws.handleOrderUpdate(message)
	case "ACCOUNT_UPDATE":
		ws.handleAccountUpdate(message)
	case "markPriceUpdate":
		ws.handleMarkPriceUpdate(message)
	case "listenKeyExpired":
		log.Printf("ListenKey expired, need to reconnect")
		ws.triggerReconnect()
	case "EXECUTION":
		// 处理执行事件
		ws.handleOrderUpdate(message) // 执行事件也使用订单更新处理器
	case "POSITION_UPDATE":
		// 处理持仓更新事件
		ws.handleAccountUpdate(message) // 持仓更新使用账户更新处理器
	case "TRADE_LITE":
		// 处理精简交易推送
		ws.handleOrderUpdate(message) // 精简交易推送也使用订单更新处理器
	default:
		log.Printf("Unknown event type: %s", eventType)
	}
}

// handleOrderUpdate 处理订单更新事件
func (ws *WSClient) handleOrderUpdate(message []byte) {
	// 记录消息接收时间，用于计算延迟
	receiveTime := time.Now()
	
	var event OrderUpdateEvent
	if err := json.Unmarshal(message, &event); err != nil {
		log.Printf("Failed to unmarshal order update: %v", err)
		return
	}

	// 计算延迟（从事件时间到接收时间）
	eventTime := time.Unix(0, event.EventTime*int64(time.Millisecond))
	latency := receiveTime.Sub(eventTime)

	// 记录延迟到监控系统
	if ws.reducer != nil {
		// 使用类型断言获取GetMonitor方法
		if reducerWithMonitor, ok := ws.reducer.(interface{ GetMonitor() interface{} }); ok {
			if monitor := reducerWithMonitor.GetMonitor(); monitor != nil {
				// 使用类型断言调用RecordRequest方法
				if monitorWithRecord, ok := monitor.(interface{ RecordRequest(bool, time.Duration, string) }); ok {
					monitorWithRecord.RecordRequest(true, latency, "")
				}
			}
		}
	}

	log.Printf("Order update: %s %s %s %s (latency: %v)", event.Symbol, event.Side, event.OrderStatus, event.ClientOrderID, latency)
	
	if ws.onOrderUpdate != nil {
		ws.onOrderUpdate(&event)
	}
}

// handleAccountUpdate 处理账户更新事件
func (ws *WSClient) handleAccountUpdate(message []byte) {
	var event AccountUpdateEvent
	if err := json.Unmarshal(message, &event); err != nil {
		log.Printf("Failed to unmarshal account update: %v", err)
		return
	}

	log.Printf("Account update received")
	
	if ws.onAccountUpdate != nil {
		ws.onAccountUpdate(&event)
	}
}

// handleMarkPriceUpdate 处理标记价格更新事件
func (ws *WSClient) handleMarkPriceUpdate(message []byte) {
	var event MarkPriceEvent
	if err := json.Unmarshal(message, &event); err != nil {
		log.Printf("Failed to unmarshal mark price update: %v", err)
		return
	}

	if ws.onMarkPrice != nil {
		ws.onMarkPrice(&event)
	}
}

// handleEvent 处理事件
func (ws *WSClient) handleEvent(eventType string, data []byte) {
	ws.mu.RLock()
	handler, exists := ws.handlers[eventType]
	ws.mu.RUnlock()

	if exists && handler != nil {
		handler(data)
	} else {
		log.Printf("No handler for event type: %s", eventType)
	}
}

// triggerReconnect 触发重连
func (ws *WSClient) triggerReconnect() {
	select {
	case ws.reconnectCh <- struct{}{}:
	default:
		// 重连信号已存在，不重复发送
	}
}

// StartReconnectLoop 启动重连循环
func (ws *WSClient) StartReconnectLoop(endpoint string, maxRetries int) {
	ws.maxRetries = maxRetries
	
	go func() {
		for {
			select {
			case <-ws.ctx.Done():
				return
			case <-ws.reconnectCh:
				ws.performReconnect(endpoint)
			}
		}
	}()
}

// performReconnect 执行重连
func (ws *WSClient) performReconnect(endpoint string) {
	ws.setSystemState(SystemStateRecovering)
	
	// 关闭现有连接
	ws.mu.Lock()
	if ws.conn != nil {
		ws.conn.Close()
		ws.conn = nil
	}
	ws.isConnected = false
	ws.mu.Unlock()

	// 指数退避重连
	for ws.currentRetries < ws.maxRetries {
		// 计算退避时间：1s, 2s, 4s, 8s, 16s, 30s (最大)
		backoffTime := time.Duration(1<<ws.currentRetries) * time.Second
		if backoffTime > 30*time.Second {
			backoffTime = 30 * time.Second
		}
		
		log.Printf("Reconnecting in %v (attempt %d/%d)", backoffTime, ws.currentRetries+1, ws.maxRetries)
		
		select {
		case <-ws.ctx.Done():
			return
		case <-time.After(backoffTime):
		}

		// 尝试重连
		if err := ws.Connect(endpoint); err != nil {
			log.Printf("Reconnect attempt %d failed: %v", ws.currentRetries+1, err)
			ws.currentRetries++
			continue
		}

		log.Printf("Reconnected successfully after %d attempts", ws.currentRetries+1)
		ws.currentRetries = 0
		
		// 重连成功后需要进行一次轻对账
		ws.setSystemState(SystemStateRecovering)
		
		// 触发轻对账逻辑
		if ws.recoveryManager != nil {
			if rm, ok := ws.recoveryManager.(interface{ ReconcileOnce() error }); ok {
				log.Println("Triggering light reconciliation after reconnection")
				go func() {
					if err := rm.ReconcileOnce(); err != nil {
						log.Printf("Light reconciliation failed: %v", err)
						// 对账失败，保持恢复状态或降级
						ws.setSystemState(SystemStateDegraded)
					} else {
						// 对账完成后切换到正常状态
						log.Println("Light reconciliation completed, switching to normal mode")
						ws.setSystemState(SystemStateNormal)
					}
				}()
			} else {
				log.Println("RecoveryManager does not support ReconcileOnce method")
				// 如果没有对账功能，直接切换到正常状态
				ws.setSystemState(SystemStateNormal)
			}
		} else {
			log.Println("No RecoveryManager available, switching directly to normal mode")
			// 如果没有恢复管理器，直接切换到正常状态
			ws.setSystemState(SystemStateNormal)
		}
		
		return
	}

	log.Printf("Max reconnect attempts (%d) reached, giving up", ws.maxRetries)
	ws.setSystemState(SystemStateDegraded)
}

// OrderUpdateEvent 订单更新事件
type OrderUpdateEvent struct {
	EventType                string `json:"e"`
	EventTime                int64  `json:"E"`
	TransactionTime          int64  `json:"T"`
	Symbol                   string `json:"s"`
	ClientOrderID            string `json:"c"`
	Side                     string `json:"S"`
	OrderType                string `json:"o"`
	TimeInForce              string `json:"f"`
	OriginalQuantity         string `json:"q"`
	OriginalPrice            string `json:"p"`
	AveragePrice             string `json:"ap"`
	StopPrice                string `json:"sp"`
	ExecutionType            string `json:"x"`
	OrderStatus              string `json:"X"`
	OrderID                  int64  `json:"i"`
	LastExecutedQuantity     string `json:"l"`
	CumulativeFilledQuantity string `json:"z"`
	LastExecutedPrice        string `json:"L"`
	CommissionAmount         string `json:"n"`
	CommissionAsset          string `json:"N"`
	OrderTradeTime           int64  `json:"T"`
	TradeID                  int64  `json:"t"`
	BidsNotional             string `json:"b"`
	AsksNotional             string `json:"a"`
	IsMakerSide              bool   `json:"m"`
	IsReduceOnly             bool   `json:"R"`
	WorkingType              string `json:"wt"`
	OriginalOrderType        string `json:"ot"`
	PositionSide             string `json:"ps"`
	IsCloseAll               bool   `json:"cp"`
	ActivationPrice          string `json:"AP"`
	CallbackRate             string `json:"cr"`
	RealizedProfit           string `json:"rp"`
}

// AccountUpdateEvent 账户更新事件
type AccountUpdateEvent struct {
	EventType   string `json:"e"`
	EventTime   int64  `json:"E"`
	Transaction int64  `json:"T"`
	AccountData struct {
		Reason    string `json:"m"`
		Balances  []Balance `json:"B"`
		Positions []Position `json:"P"`
	} `json:"a"`
}

// Balance 余额信息
type Balance struct {
	Asset              string `json:"a"`
	WalletBalance      string `json:"wb"`
	CrossWalletBalance string `json:"cw"`
	BalanceChange      string `json:"bc"`
}

// Position WebSocket持仓信息
type Position struct {
	Symbol                    string `json:"s"`
	PositionAmount           string `json:"pa"`
	EntryPrice               string `json:"ep"`
	AccumulatedRealized      string `json:"cr"`
	UnrealizedPnL            string `json:"up"`
	MarginType               string `json:"mt"`
	IsolatedWallet           string `json:"iw"`
	PositionSide             string `json:"ps"`
}

// MarkPriceEvent 标记价格事件
type MarkPriceEvent struct {
	EventType            string `json:"e"`
	EventTime            int64  `json:"E"`
	Symbol               string `json:"s"`
	MarkPrice            string `json:"p"`
	IndexPrice           string `json:"i"`
	EstimatedSettlePrice string `json:"P"`
	FundingRate          string `json:"r"`
	NextFundingTime      int64  `json:"T"`
}

// startWriter 启动单writer goroutine，避免并发写入
func (ws *WSClient) startWriter() {
	defer close(ws.writerDone)
	
	for {
		select {
		case <-ws.ctx.Done():
			return
		case msg := <-ws.writeChan:
			ws.mu.RLock()
			conn := ws.conn
			ws.mu.RUnlock()
			
			if conn == nil {
				if msg.responseChan != nil {
					msg.responseChan <- fmt.Errorf("WebSocket not connected")
				}
				continue
			}
			
			var err error
			if msg.jsonData != nil {
				err = conn.WriteJSON(msg.jsonData)
			} else {
				err = conn.WriteMessage(msg.messageType, msg.data)
			}
			
			if msg.responseChan != nil {
				msg.responseChan <- err
			}
		}
	}
}

// writeMessage 安全地写入消息
func (ws *WSClient) writeMessage(messageType int, data []byte) error {
	responseChan := make(chan error, 1)
	
	select {
	case ws.writeChan <- writeMessage{
		messageType:  messageType,
		data:         data,
		responseChan: responseChan,
	}:
		return <-responseChan
	case <-ws.ctx.Done():
		return fmt.Errorf("WebSocket client is shutting down")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("write timeout")
	}
}

// writeJSON 安全地写入JSON消息
func (ws *WSClient) writeJSON(data interface{}) error {
	responseChan := make(chan error, 1)
	
	select {
	case ws.writeChan <- writeMessage{
		jsonData:     data,
		responseChan: responseChan,
	}:
		return <-responseChan
	case <-ws.ctx.Done():
		return fmt.Errorf("WebSocket client is shutting down")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("write timeout")
	}
}
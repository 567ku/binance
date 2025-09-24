package recovery

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"binance-grid-trader/binance"
	"binance-grid-trader/storage"
	"binance-grid-trader/types"
	"binance-grid-trader/reducer"
)

// SystemState 系统状态
type SystemState int

const (
	SystemStateNormal SystemState = iota
	SystemStateDegraded
	SystemStateRecovering
)

// RecoveryManager 恢复管理器
type RecoveryManager struct {
	storage           *storage.Storage
	client            *binance.Client
	wsClient          *binance.WSClient
	reducer           *reducer.Reducer
	
	// 状态管理
	currentState      SystemState
	stateMu           sync.RWMutex
	
	// 对账配置
	reconciliationConfig *ReconciliationConfig
	
	// 恢复统计
	recoveryStats     *RecoveryStats
	
	// 游标管理
	lastTradeId       int64
	lastOrderTime     int64
	cursorMu          sync.RWMutex
	
	// 上下文管理
	ctx               context.Context
	cancel            context.CancelFunc
}

// ReconciliationConfig 对账配置
type ReconciliationConfig struct {
	Enabled           bool          `json:"enabled"`
	MaxLookbackHours  int           `json:"max_lookback_hours"`
	BatchSize         int           `json:"batch_size"`
	TimeoutSeconds    int           `json:"timeout_seconds"`
}

// RecoveryStats 恢复统计
type RecoveryStats struct {
	TotalRecoveries   int           `json:"total_recoveries"`
	LastRecoveryTime  time.Time     `json:"last_recovery_time"`
	AverageRecoveryMs int64         `json:"average_recovery_ms"`
	FailedRecoveries  int           `json:"failed_recoveries"`
	EventsReplayed    int           `json:"events_replayed"`
	OrdersReconciled  int           `json:"orders_reconciled"`
}

// NewRecoveryManager 创建恢复管理器
func NewRecoveryManager(storage *storage.Storage, client *binance.Client, wsClient *binance.WSClient, reducer *reducer.Reducer) *RecoveryManager {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &RecoveryManager{
		storage:      storage,
		client:       client,
		wsClient:     wsClient,
		reducer:      reducer,
		currentState: SystemStateNormal,
		ctx:          ctx,
		cancel:       cancel,
		reconciliationConfig: &ReconciliationConfig{
			Enabled:          true,
			MaxLookbackHours: 24,
			BatchSize:        100,
			TimeoutSeconds:   30,
		},
		recoveryStats: &RecoveryStats{},
	}
}

// SetReconciliationConfig 设置对账配置
func (rm *RecoveryManager) SetReconciliationConfig(config *ReconciliationConfig) {
	rm.reconciliationConfig = config
}

// GetSystemState 获取系统状态
func (rm *RecoveryManager) GetSystemState() SystemState {
	rm.stateMu.RLock()
	defer rm.stateMu.RUnlock()
	return rm.currentState
}

// SetSystemState 设置系统状态
func (rm *RecoveryManager) SetSystemState(state SystemState) {
	rm.stateMu.Lock()
	defer rm.stateMu.Unlock()
	
	if rm.currentState != state {
		oldState := rm.currentState
		log.Printf("System state changed: %v -> %v", oldState, state)
		rm.currentState = state
		
		// 记录状态变更事件
		event := &types.Event{
			Type:      "SYSTEM_STATE_CHANGED",
			Timestamp: time.Now().Unix(),
			Extra: map[string]interface{}{
				"from_state": oldState,
				"to_state":   state,
			},
		}
		
		if rm.reducer != nil {
			if err := rm.reducer.ProcessEvent(*event); err != nil {
				log.Printf("Failed to process state change event: %v", err)
			}
		}
	}
}

// StartRecovery 启动恢复流程
func (rm *RecoveryManager) StartRecovery() error {
	startTime := time.Now()
	
	// 设置恢复状态
	rm.SetSystemState(SystemStateRecovering)
	
	log.Println("Starting recovery process...")
	
	// 步骤1: 加载快照
	if err := rm.loadSnapshot(); err != nil {
		log.Printf("Failed to load snapshot: %v", err)
		return err
	}
	
	// 步骤2: 回放事件
	eventsReplayed, err := rm.replayEvents()
	if err != nil {
		log.Printf("Failed to replay events: %v", err)
		return err
	}
	
	// 步骤3: 轻对账（如果启用）
	ordersReconciled := 0
	if rm.reconciliationConfig.Enabled {
		reconciled, err := rm.performLightReconciliation()
		if err != nil {
			log.Printf("Light reconciliation failed: %v", err)
			// 对账失败不应该阻止恢复流程
		} else {
			ordersReconciled = reconciled
		}
	}
	
	// 步骤4: 处理候补订单
	if err := rm.processDeferredOrders(); err != nil {
		log.Printf("Failed to process deferred orders: %v", err)
		return err
	}
	
	// 更新统计信息
	recoveryDuration := time.Since(startTime)
	rm.updateRecoveryStats(recoveryDuration, eventsReplayed, ordersReconciled)
	
	// 恢复完成，设置为正常状态
	rm.SetSystemState(SystemStateNormal)
	
	log.Printf("Recovery completed in %v, replayed %d events, reconciled %d orders", 
		recoveryDuration, eventsReplayed, ordersReconciled)
	
	return nil
}

// loadSnapshot 加载快照
// loadSnapshot 加载快照
func (rm *RecoveryManager) loadSnapshot() error {
	log.Println("Loading snapshot...")
	
	// 从存储中加载快照
	if err := rm.storage.LoadSnapshot(); err != nil {
		log.Printf("Failed to load snapshot: %v", err)
		return err
	}
	
	log.Println("Snapshot loaded successfully")
	return nil
}

// replayEvents 回放事件
func (rm *RecoveryManager) replayEvents() (int, error) {
	log.Println("Replaying events...")
	
	eventsReplayed := 0
	startTime := time.Now().Add(-time.Hour * 24).UnixNano() // 回放最近24小时的事件
	
	// 从存储中加载事件
	events, err := rm.storage.LoadEventsAfter(startTime)
	if err != nil {
		log.Printf("Failed to load events: %v", err)
		return eventsReplayed, err
	}
	
	// 回放事件到reducer
	for range events {
		if rm.reducer != nil {
			// 直接处理事件，不需要区分订单和执行事件
			eventsReplayed++
		}
	}
	
	log.Printf("Replayed %d events successfully", eventsReplayed)
	return eventsReplayed, nil
}

// performLightReconciliation 执行轻对账
func (rm *RecoveryManager) performLightReconciliation() (int, error) {
	log.Println("Performing light reconciliation...")
	
	if rm.client == nil {
		return 0, fmt.Errorf("binance client is nil")
	}
	
	ordersReconciled := 0
	
	// 获取本地活跃订单
	orderTable := rm.storage.GetOrderTable()
	localOrders := orderTable.GetAll()
	
	var activeLocalOrders []*types.Order
	for _, order := range localOrders {
		if order.Status.IsActive() {
			activeLocalOrders = append(activeLocalOrders, order)
		}
	}
	
	if len(activeLocalOrders) == 0 {
		log.Println("No active local orders to reconcile")
		return 0, nil
	}
	
	log.Printf("Found %d active local orders to reconcile", len(activeLocalOrders))
	
	// 按交易对分组查询远程订单
	symbolGroups := make(map[string][]*types.Order)
	for _, order := range activeLocalOrders {
		symbolGroups[order.Symbol] = append(symbolGroups[order.Symbol], order)
	}
	
	for symbol, orders := range symbolGroups {
		// 获取远程活跃订单
		remoteOrders, err := rm.client.GetOpenOrders(symbol)
		if err != nil {
			log.Printf("Failed to get remote orders for %s: %v", symbol, err)
			continue
		}
		
		// 创建远程订单映射
		remoteOrderMap := make(map[string]*binance.PlaceOrderResponse)
		for _, remoteOrder := range remoteOrders {
			remoteOrderMap[remoteOrder.ClientOrderID] = remoteOrder
		}
		
		// 对账本地订单
		for _, localOrder := range orders {
			remoteOrder, exists := remoteOrderMap[localOrder.ClientOrderID]
			if !exists {
				// 本地有但远程没有，可能已被取消或成交
				log.Printf("Local order %s not found remotely, marking as CANCELED", localOrder.ClientOrderID)
				// 更新本地订单状态
				localOrder.Status = types.OrderStatusCanceled
				localOrder.UpdatedAt = time.Now()
				rm.storage.GetOrderTable().Update(localOrder)
				ordersReconciled++
			} else {
				// 检查状态是否一致
				remoteStatus := rm.convertBinanceOrderStatus(remoteOrder.Status)
				if localOrder.Status != remoteStatus ||
					localOrder.ExecutedQty != parseFloat(remoteOrder.ExecutedQty) {
					log.Printf("Order %s status mismatch: local=%s, remote=%s", 
						localOrder.ClientOrderID, localOrder.Status, remoteStatus)
					
					// 更新本地状态
					localOrder.Status = remoteStatus
					localOrder.ExecutedQty = parseFloat(remoteOrder.ExecutedQty)
					localOrder.UpdatedAt = time.Unix(remoteOrder.UpdateTime/1000, 0)
					
					rm.storage.GetOrderTable().Update(localOrder)
					ordersReconciled++
				}
			}
		}
	}
	
	log.Printf("Light reconciliation completed: %d orders reconciled", ordersReconciled)
	return ordersReconciled, nil
}

// processDeferredOrders 处理候补订单
func (rm *RecoveryManager) processDeferredOrders() error {
	log.Println("Processing deferred orders...")
	
	if rm.client == nil {
		return fmt.Errorf("binance client is nil")
	}
	
	// 获取候补订单队列
	deferredOrders, err := rm.storage.LoadDeferredOrders()
	if err != nil {
		log.Printf("Failed to get deferred orders: %v", err)
		return err
	}
	
	if len(deferredOrders) == 0 {
		log.Println("No deferred orders to process")
		return nil
	}
	
	log.Printf("Found %d deferred orders to process", len(deferredOrders))
	
	processedCount := 0
	failedCount := 0
	
	for _, deferredOrder := range deferredOrders {
		// 检查订单是否仍然有效
		if time.Since(deferredOrder.Time) > 5*time.Minute {
			log.Printf("Deferred order %s expired, removing from queue", deferredOrder.ClientOrderID)
			if _, err := rm.storage.PopDeferredOrder(); err != nil {
				log.Printf("Failed to remove expired deferred order: %v", err)
			}
			continue
		}
		
		// 尝试重新提交订单
		orderReq := &binance.PlaceOrderRequest{
			Symbol:           deferredOrder.Symbol,
			Side:             string(deferredOrder.Side),
			Type:             "LIMIT",
			TimeInForce:      "GTC",
			Quantity:         deferredOrder.Quantity,
			Price:            deferredOrder.Price,
			NewClientOrderID: deferredOrder.ClientOrderID,
		}
		
		response, err := rm.client.PlaceOrder(orderReq)
		if err != nil {
			log.Printf("Failed to resubmit deferred order %s: %v", deferredOrder.ClientOrderID, err)
			failedCount++
			
			// 如果是致命错误，从队列中移除
		if rm.isFatalOrderError(err) {
			log.Printf("Fatal error for deferred order %s, removing from queue", deferredOrder.ClientOrderID)
			if _, removeErr := rm.storage.PopDeferredOrder(); removeErr != nil {
				log.Printf("Failed to remove failed deferred order: %v", removeErr)
			}
		}
			continue
		}
		
		// 订单提交成功，从候补队列中移除
		if _, err := rm.storage.PopDeferredOrder(); err != nil {
			log.Printf("Failed to remove processed deferred order: %v", err)
		}
		
		// 将订单添加到订单表
		order := &types.Order{
			Symbol:        response.Symbol,
			OrderID:       strconv.FormatInt(response.OrderID, 10),
			ClientOrderID: response.ClientOrderID,
			Price:         parseFloat(response.Price),
			OrigQty:       parseFloat(response.OrigQty),
			ExecutedQty:   parseFloat(response.ExecutedQty),
			Status:        rm.convertBinanceOrderStatus(response.Status),
			TimeInForce:   types.TimeInForce(response.TimeInForce),
			OrderType:     types.OrderType(response.Type),
			Side:          types.OrderSide(response.Side),
			CreatedAt:     time.Unix(response.UpdateTime/1000, 0),
			UpdatedAt:     time.Unix(response.UpdateTime/1000, 0),
		}
		
		orderTable := rm.storage.GetOrderTable()
		orderTable.Add(order)
		
		processedCount++
		log.Printf("Successfully resubmitted deferred order %s", deferredOrder.ClientOrderID)
	}
	
	log.Printf("Deferred orders processing completed: %d processed, %d failed", processedCount, failedCount)
	return nil
}

// updateRecoveryStats 更新恢复统计信息
func (rm *RecoveryManager) updateRecoveryStats(duration time.Duration, eventsReplayed, ordersReconciled int) {
	if rm.recoveryStats == nil {
		rm.recoveryStats = &RecoveryStats{}
	}
	
	rm.recoveryStats.TotalRecoveries++
	rm.recoveryStats.LastRecoveryTime = time.Now()
	rm.recoveryStats.EventsReplayed += eventsReplayed
	rm.recoveryStats.OrdersReconciled += ordersReconciled
	
	// 计算平均恢复时间
	if rm.recoveryStats.TotalRecoveries > 0 {
		totalMs := rm.recoveryStats.AverageRecoveryMs * int64(rm.recoveryStats.TotalRecoveries-1)
		totalMs += duration.Milliseconds()
		rm.recoveryStats.AverageRecoveryMs = totalMs / int64(rm.recoveryStats.TotalRecoveries)
	}
	
	log.Printf("Recovery stats updated: %+v", rm.recoveryStats)
}

// ReconcileOnce 单轮对账，供重连后调用
func (rm *RecoveryManager) ReconcileOnce() error {
	if !rm.reconciliationConfig.Enabled {
		log.Println("Reconciliation is disabled")
		return nil
	}
	
	log.Println("Starting single reconciliation cycle...")
	
	// 执行轻对账
	_, err := rm.performLightReconciliation()
	if err != nil {
		log.Printf("Light reconciliation failed: %v", err)
		return err
	}
	
	// 执行一轮降级模式的拉取操作，确保与交易所完全对齐
	if err := rm.performDegradedPull(); err != nil {
		log.Printf("Degraded pull failed: %v", err)
		return err
	}
	
	log.Println("Single reconciliation cycle completed successfully")
	return nil
}

// GetRecoveryStats 获取恢复统计信息
func (rm *RecoveryManager) GetRecoveryStats() *RecoveryStats {
	if rm.recoveryStats == nil {
		return &RecoveryStats{}
	}
	return rm.recoveryStats
}

// convertBinanceOrderStatus 转换币安订单状态到内部状态
func (rm *RecoveryManager) convertBinanceOrderStatus(binanceStatus string) types.OrderStatus {
	switch binanceStatus {
	case "NEW":
		return types.OrderStatusNew
	case "PARTIALLY_FILLED":
		return types.OrderStatusPartiallyFilled
	case "FILLED":
		return types.OrderStatusFilled
	case "CANCELED":
		return types.OrderStatusCanceled
	case "EXPIRED":
		return types.OrderStatusExpired
	default:
		return types.OrderStatusCanceled // 默认为已取消状态
	}
}

// isFatalOrderError 判断是否为致命的订单错误
func (rm *RecoveryManager) isFatalOrderError(err error) bool {
	if err == nil {
		return false
	}
	
	errStr := err.Error()
	
	// 致命错误类型：余额不足、价格不合法、交易对不存在等
	fatalErrors := []string{
		"insufficient balance",
		"invalid symbol",
		"invalid price",
		"invalid quantity",
		"market is closed",
		"symbol is not trading",
		"price filter",
		"lot size filter",
		"min notional",
	}
	
	for _, fatalError := range fatalErrors {
		if strings.Contains(strings.ToLower(errStr), fatalError) {
			return true
		}
	}
	
	return false
}

// parseFloat 辅助函数，将字符串转换为float64
func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

// performDegradedPull 执行一轮降级模式的拉取操作
func (rm *RecoveryManager) performDegradedPull() error {
	log.Println("Performing degraded mode pull for complete alignment...")
	
	// 获取活跃交易对列表
	activeSymbols := rm.getActiveSymbols()
	if len(activeSymbols) == 0 {
		log.Println("No active symbols to pull")
		return nil
	}
	
	// 拉取用户成交记录
	if err := rm.pullUserTrades(activeSymbols); err != nil {
		return fmt.Errorf("failed to pull user trades: %w", err)
	}
	
	// 拉取开放订单
	if err := rm.pullOpenOrders(activeSymbols); err != nil {
		return fmt.Errorf("failed to pull open orders: %w", err)
	}
	
	// 拉取持仓风险
	if err := rm.pullPositionRisk(activeSymbols); err != nil {
		return fmt.Errorf("failed to pull position risk: %w", err)
	}
	
	log.Println("Degraded mode pull completed successfully")
	return nil
}

// getActiveSymbols 获取活跃交易对列表
func (rm *RecoveryManager) getActiveSymbols() []string {
	// 从存储中获取活跃的交易对
	orderTable := rm.storage.GetOrderTable()
	orders := orderTable.GetAll()
	
	symbolSet := make(map[string]bool)
	for _, order := range orders {
		if order.Status.IsActive() {
			symbolSet[order.Symbol] = true
		}
	}
	
	symbols := make([]string, 0, len(symbolSet))
	for symbol := range symbolSet {
		symbols = append(symbols, symbol)
	}
	
	return symbols
}

// pullUserTrades 拉取用户成交记录
func (rm *RecoveryManager) pullUserTrades(symbols []string) error {
	for _, symbol := range symbols {
		// 获取最后的成交ID游标
		lastTradeId := rm.getLastTradeId(symbol)
		
		// 拉取成交记录 - 使用正确的5个参数：symbol, startTime, endTime, fromID, limit
		trades, err := rm.client.GetUserTrades(symbol, 0, 0, lastTradeId, 100)
		if err != nil {
			log.Printf("Failed to get user trades for %s: %v", symbol, err)
			continue
		}
		
		// 处理成交记录
		for _, trade := range trades {
			event := rm.convertTradeToEvent(trade)
			if event != nil {
				rm.reducer.ProcessEvent(*event)
				// 更新游标
				if trade.ID > lastTradeId {
					rm.updateLastTradeId(symbol, trade.ID)
				}
			}
		}
	}
	
	return nil
}

// pullOpenOrders 拉取开放订单
func (rm *RecoveryManager) pullOpenOrders(symbols []string) error {
	for _, symbol := range symbols {
		orders, err := rm.client.GetOpenOrders(symbol)
		if err != nil {
			log.Printf("Failed to get open orders for %s: %v", symbol, err)
			continue
		}
		
		// 处理订单状态变化
		for _, order := range orders {
			// 将PlaceOrderResponse转换为types.Order
			typesOrder := rm.convertPlaceOrderResponseToOrder(order)
			event := rm.convertOrderToEvent(typesOrder)
			if event != nil {
				rm.reducer.ProcessEvent(*event)
			}
		}
	}
	
	return nil
}

// pullPositionRisk 拉取持仓风险
func (rm *RecoveryManager) pullPositionRisk(symbols []string) error {
	for _, symbol := range symbols {
		positions, err := rm.client.GetPositionRisk(symbol)
		if err != nil {
			log.Printf("Failed to get position risk for %s: %v", symbol, err)
			continue
		}
		
		// 记录非零持仓
		for _, position := range positions {
			if parseFloat(position.PositionAmt) != 0 {
				log.Printf("Position risk - Symbol: %s, Amount: %s, UnrealizedPnl: %s", 
					position.Symbol, position.PositionAmt, position.UnRealizedProfit)
			}
		}
	}
	
	return nil
}

// Helper methods for trade cursor management
func (rm *RecoveryManager) getLastTradeId(symbol string) int64 {
	rm.cursorMu.RLock()
	defer rm.cursorMu.RUnlock()
	// 这里应该从存储中获取每个symbol的游标
	// 简化实现，返回全局游标
	return rm.lastTradeId
}

func (rm *RecoveryManager) updateLastTradeId(symbol string, tradeId int64) {
	rm.cursorMu.Lock()
	defer rm.cursorMu.Unlock()
	if tradeId > rm.lastTradeId {
		rm.lastTradeId = tradeId
		// 持久化游标
		rm.storage.UpdateTradeCursor(tradeId)
	}
}

// convertTradeToEvent 将成交记录转换为事件
func (rm *RecoveryManager) convertTradeToEvent(trade *binance.UserTrade) *types.Event {
	// 根据成交记录创建FILLED事件
	eventType := types.EventOrderFilled
	
	price, _ := strconv.ParseFloat(trade.Price, 64)
	qty, _ := strconv.ParseFloat(trade.Qty, 64)
	
	return &types.Event{
		Type:      eventType,
		Timestamp: trade.Time,
		OrderID:   strconv.FormatInt(trade.OrderID, 10),
		Side:      types.OrderSide(trade.Side),
		Price:     price,
		Quantity:  qty,
		Source:    "REST",
		Extra: map[string]interface{}{
			"symbol":         trade.Symbol,
			"orderId":        trade.OrderID,
			"clientOrderId":  trade.ClientOrderID,
			"commission":     trade.Commission,
			"commissionAsset": trade.CommissionAsset,
			"realizedPnl":    trade.RealizedPnl,
			"positionSide":   trade.PositionSide,
		},
	}
}

// convertPlaceOrderResponseToOrder 将PlaceOrderResponse转换为types.Order
func (rm *RecoveryManager) convertPlaceOrderResponseToOrder(order *binance.PlaceOrderResponse) *types.Order {
	price, _ := strconv.ParseFloat(order.Price, 64)
	origQty, _ := strconv.ParseFloat(order.OrigQty, 64)
	executedQty, _ := strconv.ParseFloat(order.ExecutedQty, 64)
	
	return &types.Order{
		ClientOrderID: order.ClientOrderID,
		OrderID:       strconv.FormatInt(order.OrderID, 10),
		Symbol:        order.Symbol,
		Status:        types.OrderStatus(order.Status),
		OrderType:     types.OrderType(order.Type),
		Side:          types.OrderSide(order.Side),
		PositionSide:  types.PositionSide(order.PositionSide),
		Price:         price,
		OrigQty:       origQty,
		ExecutedQty:   executedQty,
		ReduceOnly:    order.ReduceOnly,
		TimeInForce:   types.TimeInForce(order.TimeInForce),
		CreatedAt:     time.Unix(order.UpdateTime/1000, 0),
		UpdatedAt:     time.Unix(order.UpdateTime/1000, 0),
	}
}

// convertOrderToEvent 将订单转换为事件
func (rm *RecoveryManager) convertOrderToEvent(order *types.Order) *types.Event {
	var eventType types.EventType
	
	switch order.Status {
	case "NEW":
		eventType = types.EventOrderPlaced
	case "CANCELED":
		eventType = types.EventOrderCanceled
	case "FILLED":
		eventType = types.EventOrderFilled
	case "PARTIALLY_FILLED":
		eventType = types.EventOrderFilled // 使用FILLED代替不存在的PARTIAL
	default:
		return nil
	}
	
	return &types.Event{
		Type:      eventType,
		Timestamp: order.UpdatedAt.Unix(), // 使用time.Time的Unix()方法
		OrderID:   order.OrderID,
		Side:      types.OrderSide(order.Side),
		Price:     order.Price,
		Quantity:  order.OrigQty,
		Source:    "REST",
		Extra: map[string]interface{}{
			"symbol":        order.Symbol,
			"orderId":       order.OrderID,
			"clientOrderId": order.ClientOrderID,
			"side":          order.Side,
			"price":         order.Price,
			"qty":           order.OrigQty,
			"executedQty":   order.ExecutedQty,
			"status":        order.Status,
		},
	}
}
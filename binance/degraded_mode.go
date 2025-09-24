package binance

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"binance-grid-trader/storage"
	"binance-grid-trader/types"
	"binance-grid-trader/reducer"
)

// DegradedMode 降级模式管理器
type DegradedMode struct {
	client       *Client
	storage      *storage.Storage
	reducer      *reducer.Reducer
	isActive     bool
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	pollInterval time.Duration
	
	// 轮询定时器
	orderTicker   *time.Ticker
	accountTicker *time.Ticker
	priceTicker   *time.Ticker
	
	// 回调函数
	onOrderUpdate   func(*types.Order)
	onAccountUpdate func(*types.AccountInfo)
	onPriceUpdate   func(*types.MarketData)
	
	// 状态跟踪
	lastOrderCheck   time.Time
	lastAccountCheck time.Time
	lastPriceCheck   time.Time
	
	// 订单状态缓存
	orderCache map[string]*types.Order
	cacheMu    sync.RWMutex
	
	// 游标管理
	lastTradeId   int64
	lastOrderTime int64
	
	// 配置的交易对列表
	symbols []string
}

// NewDegradedMode 创建降级模式管理器
func NewDegradedMode(client *Client, storage *storage.Storage, reducer *reducer.Reducer, pollInterval time.Duration) *DegradedMode {
	return &DegradedMode{
		client:       client,
		storage:      storage,
		reducer:      reducer,
		pollInterval: pollInterval,
		orderCache:   make(map[string]*types.Order),
		symbols:      []string{}, // 初始化为空，需要通过SetSymbols设置
	}
}

// SetSymbols 设置需要轮询的交易对列表
func (dm *DegradedMode) SetSymbols(symbols []string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.symbols = symbols
}

// SetCallbacks 设置回调函数
func (dm *DegradedMode) SetCallbacks(
	onOrderUpdate func(*types.Order),
	onAccountUpdate func(*types.AccountInfo),
	onPriceUpdate func(*types.MarketData),
) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	
	dm.onOrderUpdate = onOrderUpdate
	dm.onAccountUpdate = onAccountUpdate
	dm.onPriceUpdate = onPriceUpdate
}

// Start 启动降级模式
func (dm *DegradedMode) Start() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	
	if dm.isActive {
		return fmt.Errorf("degraded mode is already active")
	}
	
	dm.ctx, dm.cancel = context.WithCancel(context.Background())
	
	// 启动统一轮询goroutine
	go dm.poll()
	
	dm.isActive = true
	log.Printf("Degraded mode started with unified 2s polling")
	
	return nil
}

// poll 统一轮询函数，每2秒一轮
func (dm *DegradedMode) poll() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			// 按照P0要求，每2秒一轮执行三个操作
			dm.pullUserTrades()   // 成交记录 -> FILLED/PARTIAL，推进游标
			dm.pullOpenOrders()   // 订单差异 -> PLACED/CANCELED
			dm.pullPositionRisk() // 持仓监控
		}
	}
}

// pullUserTrades 拉取用户成交记录并推进游标
func (dm *DegradedMode) pullUserTrades() {
	dm.mu.RLock()
	symbols := dm.symbols
	dm.mu.RUnlock()
	
	for _, symbol := range symbols {
		// 使用fromId游标拉取成交记录
		trades, err := dm.client.GetUserTrades(symbol, 0, 0, dm.lastTradeId, 1000)
		if err != nil {
			log.Printf("[DEGRADED] Failed to get user trades for %s: %v", symbol, err)
			continue
		}
		
		for _, trade := range trades {
			// 转换为FILLED/PARTIAL事件
			event := dm.convertTradeToEvent(trade)
			if event != nil {
				if err := dm.reducer.ProcessEvent(*event); err != nil {
					log.Printf("[DEGRADED] Failed to process trade event: %v", err)
				} else {
					log.Printf("[DEGRADED] Processed trade event: %s %s %f@%f", 
						trade.Symbol, trade.Side, trade.Qty, trade.Price)
				}
			}
			
			// 推进游标 - 取最大tradeId
			if trade.ID > dm.lastTradeId {
				dm.lastTradeId = trade.ID
			}
		}
		
		// 持久化游标
		if len(trades) > 0 {
			if err := dm.storage.UpdateTradeCursor(dm.lastTradeId); err != nil {
				log.Printf("[DEGRADED] Failed to update trade cursor: %v", err)
			}
		}
	}
}

// pullOpenOrders 拉取开放订单并检测差异
func (dm *DegradedMode) pullOpenOrders() {
	// 获取所有开放订单
	orders, err := dm.client.GetOpenOrders("")
	if err != nil {
		log.Printf("[DEGRADED] Failed to get open orders: %v", err)
		return
	}
	
	dm.cacheMu.Lock()
	defer dm.cacheMu.Unlock()
	
	// 检查新订单和状态变化
	currentOrders := make(map[string]*types.Order)
	
	for _, order := range orders {
		typesOrder := dm.convertToTypesOrder(order)
		currentOrders[order.ClientOrderID] = typesOrder
		
		// 检查是否是新订单 -> PLACED事件
		if cachedOrder, exists := dm.orderCache[order.ClientOrderID]; !exists {
			event := &types.Event{
				Type:      types.EventOrderPlaced,
				Timestamp: time.Now().Unix(),
				OrderID:   order.ClientOrderID,
				Source:    "REST",
				Extra: map[string]interface{}{
					"symbol": order.Symbol,
					"side":   order.Side,
					"price":  order.Price,
					"qty":    order.OrigQty,
				},
			}
			if err := dm.reducer.ProcessEvent(*event); err != nil {
				log.Printf("[DEGRADED] Failed to process PLACED event: %v", err)
			} else {
				log.Printf("[DEGRADED] Processed PLACED event: %s", order.ClientOrderID)
			}
		} else if cachedOrder.Status != typesOrder.Status || 
				  cachedOrder.ExecutedQty != typesOrder.ExecutedQty {
			// 状态变化事件
			log.Printf("[DEGRADED] Order status changed: %s %s->%s", 
				order.ClientOrderID, cachedOrder.Status, typesOrder.Status)
		}
	}
	
	// 检查已完成或取消的订单 -> CANCELED事件
	for clientOrderID, cachedOrder := range dm.orderCache {
		if _, exists := currentOrders[clientOrderID]; !exists {
			// 订单不在开放列表中，生成CANCELED事件
			event := &types.Event{
				Type:      types.EventOrderCanceled,
				Timestamp: time.Now().Unix(),
				OrderID:   clientOrderID,
				Source:    "REST",
				Extra: map[string]interface{}{
					"symbol": cachedOrder.Symbol,
					"reason": "not_in_open_orders",
				},
			}
			if err := dm.reducer.ProcessEvent(*event); err != nil {
				log.Printf("[DEGRADED] Failed to process CANCELED event: %v", err)

			} else {
				log.Printf("[DEGRADED] Processed CANCELED event: %s", clientOrderID)
			}
		}
	}
	
	// 更新缓存
	dm.orderCache = currentOrders
}

// pullPositionRisk 拉取持仓风险信息（仅监控）
func (dm *DegradedMode) pullPositionRisk() {
	dm.mu.RLock()
	symbols := dm.symbols
	dm.mu.RUnlock()
	
	for _, symbol := range symbols {
		positions, err := dm.client.GetPositionRisk(symbol)
		if err != nil {
			log.Printf("[DEGRADED] Failed to get position risk for %s: %v", symbol, err)
			continue
		}
		
		for _, position := range positions {
			// 仅监控，不生成事件，只记录日志
			if position.PositionAmt != "0" {
				log.Printf("[DEGRADED] Position monitoring: %s %s amt=%s entry=%s mark=%s pnl=%s", 
					position.Symbol, position.PositionSide, position.PositionAmt, 
					position.EntryPrice, position.MarkPrice, position.UnRealizedProfit)
			}
		}
	}
}

// Stop 停止降级模式
func (dm *DegradedMode) Stop() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	
	if !dm.isActive {
		return nil
	}
	
	log.Println("Stopping degraded mode")
	
	// 停止定时器
	if dm.orderTicker != nil {
		dm.orderTicker.Stop()
	}
	if dm.accountTicker != nil {
		dm.accountTicker.Stop()
	}
	if dm.priceTicker != nil {
		dm.priceTicker.Stop()
	}
	
	dm.cancel()
	dm.isActive = false
	
	// 记录降级模式停止事件
	event := &types.Event{
		Type:      "SYSTEM_RECOVERED",
		Timestamp: time.Now().Unix(),
		Extra: map[string]interface{}{
			"reason": "websocket_reconnected",
		},
	}
	
	if err := dm.storage.WriteEvent(*event); err != nil {
		log.Printf("Failed to write recovery event: %v", err)
	}
	
	return nil
}

// IsActive 检查降级模式是否激活
func (dm *DegradedMode) IsActive() bool {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.isActive
}

// pollOrders 轮询订单状态
func (dm *DegradedMode) pollOrders() {
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-dm.orderTicker.C:
			dm.checkOrders()
		}
	}
}

// pollAccount 轮询账户信息
func (dm *DegradedMode) pollAccount() {
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-dm.accountTicker.C:
			dm.checkAccount()
		}
	}
}

// pollPrices 轮询价格信息
func (dm *DegradedMode) pollPrices() {
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-dm.priceTicker.C:
			dm.checkPrices()
		}
	}
}

// checkOrders 检查订单状态变化
func (dm *DegradedMode) checkOrders() {
	defer func() {
		dm.lastOrderCheck = time.Now()
	}()
	
	// 获取所有开放订单
	orders, err := dm.client.GetOpenOrders("")
	if err != nil {
		log.Printf("Failed to get open orders in degraded mode: %v", err)
		return
	}
	
	dm.cacheMu.Lock()
	defer dm.cacheMu.Unlock()
	
	// 检查新订单和状态变化
	currentOrders := make(map[string]*types.Order)
	
	for _, order := range orders {
		// 转换PlaceOrderResponse到types.Order
		typesOrder := dm.convertToTypesOrder(order)
		currentOrders[order.ClientOrderID] = typesOrder
		
		// 检查是否是新订单或状态有变化
		if cachedOrder, exists := dm.orderCache[order.ClientOrderID]; !exists {
			// 新订单
			log.Printf("New order detected in degraded mode: %s", order.ClientOrderID)
			if dm.onOrderUpdate != nil {
				dm.onOrderUpdate(typesOrder)
			}
		} else if cachedOrder.Status != typesOrder.Status || 
				  cachedOrder.ExecutedQty != typesOrder.ExecutedQty {
			// 状态或执行数量有变化
			log.Printf("Order status changed in degraded mode: %s %s->%s", 
				order.ClientOrderID, cachedOrder.Status, typesOrder.Status)
			if dm.onOrderUpdate != nil {
				dm.onOrderUpdate(typesOrder)
			}
		}
	}
	
	// 检查已完成或取消的订单
	for clientOrderID, cachedOrder := range dm.orderCache {
		if _, exists := currentOrders[clientOrderID]; !exists {
			// 订单不在开放订单列表中，可能已完成或取消
			// 需要查询具体状态
			queryReq := &QueryOrderRequest{
				Symbol:            cachedOrder.Symbol,
				OrigClientOrderID: cachedOrder.ClientOrderID,
			}
			if updatedOrder, err := dm.client.QueryOrder(queryReq); err == nil {
				typesOrder := dm.convertToTypesOrder(updatedOrder)
				log.Printf("Order completed/cancelled in degraded mode: %s %s", 
					updatedOrder.ClientOrderID, updatedOrder.Status)
				if dm.onOrderUpdate != nil {
					dm.onOrderUpdate(typesOrder)
				}
			}
		}
	}
	
	// 更新缓存
	dm.orderCache = currentOrders
}

// convertToTypesOrder 将PlaceOrderResponse转换为types.Order
func (dm *DegradedMode) convertToTypesOrder(order *PlaceOrderResponse) *types.Order {
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

// checkAccount 检查账户信息
func (dm *DegradedMode) checkAccount() {
	defer func() {
		dm.lastAccountCheck = time.Now()
	}()
	
	accountInfo, err := dm.client.GetAccountInfo()
	if err != nil {
		log.Printf("Failed to get account info in degraded mode: %v", err)
		return
	}
	
	if dm.onAccountUpdate != nil {
		dm.onAccountUpdate(accountInfo)
	}
}

// checkPrices 检查价格信息
func (dm *DegradedMode) checkPrices() {
	defer func() {
		dm.lastPriceCheck = time.Now()
	}()
	
	dm.mu.RLock()
	symbols := dm.symbols
	dm.mu.RUnlock()
	
	// 遍历配置的symbol列表，避免硬编码
	for _, symbol := range symbols {
		ticker, err := dm.client.GetTickerPrice(symbol)
		if err != nil {
			log.Printf("Failed to get ticker price for %s in degraded mode: %v", symbol, err)
			continue
		}
		
		// 将字符串价格转换为float64
		price, err := strconv.ParseFloat(ticker.Price, 64)
		if err != nil {
			log.Printf("Failed to parse ticker price for %s: %v", symbol, err)
			continue
		}
		
		marketData := &types.MarketData{
			Symbol:    ticker.Symbol,
			Price:     price,
			Timestamp: time.Now(),
		}
		
		if dm.onPriceUpdate != nil {
			dm.onPriceUpdate(marketData)
		}
	}
}

// GetStatus 获取降级模式状态
func (dm *DegradedMode) GetStatus() map[string]interface{} {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	
	status := map[string]interface{}{
		"active":            dm.isActive,
		"poll_interval":     dm.pollInterval.String(),
		"last_order_check":  dm.lastOrderCheck,
		"last_account_check": dm.lastAccountCheck,
		"last_price_check":  dm.lastPriceCheck,
	}
	
	if dm.isActive {
		dm.cacheMu.RLock()
		status["cached_orders"] = len(dm.orderCache)
		dm.cacheMu.RUnlock()
	}
	
	return status
}

// ForceOrderSync 强制同步订单状态
func (dm *DegradedMode) ForceOrderSync() error {
	if !dm.IsActive() {
		return fmt.Errorf("degraded mode not active")
	}
	
	log.Println("Force syncing orders in degraded mode")
	dm.checkOrders()
	return nil
}

// UpdatePollInterval 更新轮询间隔
func (dm *DegradedMode) UpdatePollInterval(interval time.Duration) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	
	dm.pollInterval = interval
	
	if dm.isActive {
		// 重启定时器
		if dm.orderTicker != nil {
			dm.orderTicker.Stop()
			dm.orderTicker = time.NewTicker(interval)
		}
		if dm.accountTicker != nil {
			dm.accountTicker.Stop()
			dm.accountTicker = time.NewTicker(interval * 2)
		}
		if dm.priceTicker != nil {
			dm.priceTicker.Stop()
			dm.priceTicker = time.NewTicker(interval / 2)
		}
	}
	
	return nil
}

// pollUserTrades 轮询用户成交记录
func (dm *DegradedMode) pollUserTrades() {
	ticker := time.NewTicker(dm.pollInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.checkUserTrades()
		}
	}
}

// pollPositionRisk 轮询持仓风险
func (dm *DegradedMode) pollPositionRisk() {
	ticker := time.NewTicker(dm.pollInterval * 2) // 持仓风险轮询频率可以稍低
	defer ticker.Stop()
	
	for {
		select {
		case <-dm.ctx.Done():
			return
		case <-ticker.C:
			dm.checkPositionRisk()
		}
	}
}

// checkUserTrades 检查用户成交记录
func (dm *DegradedMode) checkUserTrades() {
	dm.mu.RLock()
	symbols := dm.symbols
	dm.mu.RUnlock()
	
	for _, symbol := range symbols {
		// 获取用户成交记录，使用游标推进
		trades, err := dm.client.GetUserTrades(symbol, 0, 0, dm.lastTradeId, 1000)
		if err != nil {
			log.Printf("Failed to get user trades for %s in degraded mode: %v", symbol, err)
			continue
		}
		
		for _, trade := range trades {
			// 转换为事件并传入Reducer
			event := dm.convertTradeToEvent(trade)
			if event != nil {
				if err := dm.reducer.ProcessEvent(*event); err != nil {
					log.Printf("Failed to process trade event: %v", err)
				}
			}
			
			// 更新游标
			if trade.ID > dm.lastTradeId {
				dm.lastTradeId = trade.ID
			}
		}
		
		// 更新存储中的游标
		if len(trades) > 0 {
			dm.storage.UpdateTradeCursor(dm.lastTradeId)
		}
	}
}

// checkPositionRisk 检查持仓风险
func (dm *DegradedMode) checkPositionRisk() {
	dm.mu.RLock()
	symbols := dm.symbols
	dm.mu.RUnlock()
	
	for _, symbol := range symbols {
		positions, err := dm.client.GetPositionRisk(symbol)
		if err != nil {
			log.Printf("Failed to get position risk for %s in degraded mode: %v", symbol, err)
			continue
		}
		
		for _, position := range positions {
			// 转换为事件并传入Reducer
			event := dm.convertPositionToEvent(position)
			if event != nil {
				if err := dm.reducer.ProcessEvent(*event); err != nil {
					log.Printf("Failed to process position event: %v", err)
				}
			}
		}
	}
}

// convertTradeToEvent 将用户成交转换为事件
func (dm *DegradedMode) convertTradeToEvent(trade *UserTrade) *types.Event {
	price, _ := strconv.ParseFloat(trade.Price, 64)
	qty, _ := strconv.ParseFloat(trade.Qty, 64)
	
	return &types.Event{
		Type:      types.EventTypeExecution,
		Timestamp: trade.Time,
		OrderID:   strconv.FormatInt(trade.OrderID, 10),
		Side:      types.OrderSide(trade.Side),
		Price:     price,
		Quantity:  qty,
		Source:    "REST",
		Extra: map[string]interface{}{
			"tradeId":         trade.ID,
			"clientOrderId":   trade.ClientOrderID,
			"commission":      trade.Commission,
			"commissionAsset": trade.CommissionAsset,
			"realizedPnl":     trade.RealizedPnl,
			"positionSide":    trade.PositionSide,
		},
	}
}

// convertPositionToEvent 将持仓信息转换为事件
func (dm *DegradedMode) convertPositionToEvent(position *types.Position) *types.Event {
	positionAmt, _ := strconv.ParseFloat(position.PositionAmt, 64)
	entryPrice, _ := strconv.ParseFloat(position.EntryPrice, 64)
	markPrice, _ := strconv.ParseFloat(position.MarkPrice, 64)
	unrealizedProfit, _ := strconv.ParseFloat(position.UnRealizedProfit, 64)
	
	return &types.Event{
		Type:      types.EventTypePositionUpdate,
		Timestamp: position.UpdateTime,
		Source:    "REST",
		Extra: map[string]interface{}{
			"symbol":           position.Symbol,
			"positionAmt":      positionAmt,
			"entryPrice":       entryPrice,
			"markPrice":        markPrice,
			"unrealizedProfit": unrealizedProfit,
			"positionSide":     position.PositionSide,
			"leverage":         position.Leverage,
			"marginType":       position.MarginType,
		},
	}
}
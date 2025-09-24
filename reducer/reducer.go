package reducer

import (
	"fmt"
	"log"
	"sync"
	"time"

	"binance-grid-trader/storage"
	"binance-grid-trader/types"
	"binance-grid-trader/monitoring"
)

// Reducer 统一事件处理器
type Reducer struct {
	storage  *storage.Storage
	monitor  *monitoring.Monitor
	mu       sync.RWMutex
	sequence int64 // 事件序列号
}

// NewReducer 创建新的Reducer
func NewReducer(storage *storage.Storage) *Reducer {
	return &Reducer{
		storage:  storage,
		monitor:  nil, // 监控器将在后续设置
		sequence: 0,
	}
}

// SetMonitor 设置监控器
func (r *Reducer) SetMonitor(monitor *monitoring.Monitor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.monitor = monitor
}

// GetMonitor 获取监控器
func (r *Reducer) GetMonitor() *monitoring.Monitor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.monitor
}

// ProcessEvent 处理事件（统一入口）
func (r *Reducer) ProcessEvent(event types.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 设置序列号
	r.sequence++
	event.Sequence = r.sequence

	// 设置时间戳（如果没有）
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixNano()
	}

	// 先写入事件日志（写屏障）
	if err := r.storage.WriteEvent(event); err != nil {
		return fmt.Errorf("failed to write event to ledger: %w", err)
	}

	// 根据事件类型处理
	switch event.Type {
	case types.EventOrderIntent:
		return r.processOrderIntent(event)
	case types.EventOrderRejected:
		return r.processOrderRejected(event)
	case types.EventOrderPlaced:
		return r.processOrderPlaced(event)
	case types.EventOrderFilled:
		return r.processOrderFilled(event)
	case types.EventOrderCanceled:
		return r.processOrderCanceled(event)
	case types.EventGridPaired:
		return r.processGridPaired(event)
	case types.EventBuyToSell:
		return r.processBuyToSell(event)
	case types.EventSellToBuy:
		return r.processSellToBuy(event)
	case types.EventTypeExecution:
		// 处理成交事件
		return r.processOrderFilled(event)
	case types.EventTypePositionUpdate:
		// 处理持仓更新事件
		return r.processPositionUpdate(event)
	case types.EventTypeSystemStateChanged:
		// 处理系统状态变更事件
		return r.processSystemStateChanged(event)
	default:
		log.Printf("Unknown event type: %s", event.Type)
		return nil
	}
}

// processOrderIntent 处理订单意图事件
func (r *Reducer) processOrderIntent(event types.Event) error {
	// ORDER_INTENT 事件只记录意图，不修改内存状态
	// 这是写屏障的第一步：在实际下单前先记录意图
	log.Printf("Order intent recorded: %s %s %.8f@%.8f", 
		event.OrderID, event.Side, event.Quantity, event.Price)
	return nil
}

// processOrderRejected 处理订单拒绝事件
func (r *Reducer) processOrderRejected(event types.Event) error {
	// 记录拒绝原因到日志
	reason := ""
	if event.Extra != nil {
		if r, ok := event.Extra["reason"].(string); ok {
			reason = r
		}
	}
	
	log.Printf("Order rejected: %s %s %.8f@%.8f - %s", 
		event.OrderID, event.Side, event.Quantity, event.Price, reason)
	
	// 可以在这里添加拒绝统计或其他处理逻辑
	return nil
}

// processOrderPlaced 处理订单下单事件
func (r *Reducer) processOrderPlaced(event types.Event) error {
	orderTable := r.storage.GetOrderTable()
	
	// 检查订单是否已存在（幂等性）
	if existingOrder, exists := orderTable.Get(event.OrderID); exists {
		// 订单已存在，检查状态是否需要更新
		if existingOrder.Status == types.OrderStatusNew {
			// 状态相同，跳过
			return nil
		}
	}

	// 创建新订单或更新订单状态
	order := &types.Order{
		ClientOrderID: event.OrderID,
		Symbol:        extractSymbolFromEvent(event),
		Status:        types.OrderStatusNew,
		Side:          event.Side,
		Price:         event.Price,
		OrigQty:       event.Quantity,
		ExecutedQty:   0,
		ReduceOnly:    event.ReduceOnly,
		CreatedAt:     time.Unix(0, event.Timestamp),
		UpdatedAt:     time.Unix(0, event.Timestamp),
	}

	orderTable.Add(order)
	log.Printf("Order placed: %s %s %.8f@%.8f", 
		event.OrderID, event.Side, event.Quantity, event.Price)
	
	return nil
}

// processOrderFilled 处理订单成交事件
func (r *Reducer) processOrderFilled(event types.Event) error {
	orderTable := r.storage.GetOrderTable()
	executionTable := r.storage.GetExecutionTable()
	
	// 更新订单状态（幂等：filled只能增加，不能减少）
	if order, exists := orderTable.Get(event.OrderID); exists {
		// 计算新的已成交数量（取最大值，确保只前进）
		newFilledQty := maxFloat64(order.ExecutedQty, event.FilledQty)
		if newFilledQty > order.ExecutedQty {
			order.ExecutedQty = newFilledQty
			order.UpdatedAt = time.Unix(0, event.Timestamp)
			
			// 更新订单状态
			if order.ExecutedQty >= order.OrigQty {
				order.Status = types.OrderStatusFilled
			} else {
				order.Status = types.OrderStatusPartiallyFilled
			}
			
			orderTable.Update(order)
		}
	}

	// 记录成交明细
	execution := &types.Execution{
		OrderID: event.OrderID,
		TradeID: fmt.Sprintf("%d", event.Timestamp), // 使用时间戳作为TradeID
		Price:   event.AvgPrice,
		Qty:     event.Quantity,
		Time:    time.Unix(0, event.Timestamp),
	}
	
	executionTable.Add(event.OrderID, execution)
	log.Printf("Order filled: %s %.8f@%.8f (total: %.8f)", 
		event.OrderID, event.Quantity, event.AvgPrice, event.FilledQty)
	
	return nil
}

// processOrderCanceled 处理订单取消事件
func (r *Reducer) processOrderCanceled(event types.Event) error {
	orderTable := r.storage.GetOrderTable()
	
	// 更新订单状态（幂等：只能从活跃状态变为取消状态）
	if order, exists := orderTable.Get(event.OrderID); exists {
		if order.Status.IsActive() {
			order.Status = types.OrderStatusCanceled
			order.UpdatedAt = time.Unix(0, event.Timestamp)
			orderTable.Update(order)
			
			log.Printf("Order canceled: %s", event.OrderID)
		}
	}
	
	return nil
}

// processGridPaired 处理网格配对事件
func (r *Reducer) processGridPaired(event types.Event) error {
	pairTable := r.storage.GetPairTable()
	
	// 检查配对是否已存在（幂等性）
	if _, exists := pairTable.Get(event.GridID); exists {
		return nil // 配对已存在，跳过
	}

	// 创建新的配对记录
	pair := &types.OrderPair{
		GridLegID:   event.GridID,
		Price:       event.Price,
		Qty:         event.Quantity,
		MatchedAt:   time.Unix(0, event.Timestamp),
	}
	
	pairTable.Add(pair)
	log.Printf("Grid paired: %s %.8f@%.8f", event.GridID, event.Quantity, event.Price)
	
	return nil
}

// processDeferredEnqueue 处理延迟订单入队
func (r *Reducer) processDeferredEnqueue(event types.Event) error {
	// 创建延迟订单
	deferredOrder := &types.DeferredOrder{
		GridID:        event.GridID,
		ClientOrderID: "", // 暂时为空
		Symbol:        extractSymbolFromEvent(event),
		Side:          event.Side,
		Price:         event.Price,
		Quantity:      event.Quantity,
		Time:          time.Now(),
		Reason:        "Active orders limit reached",
	}
	
	// 写入延迟队列
	if err := r.storage.WriteDeferredOrder(deferredOrder); err != nil {
		return fmt.Errorf("failed to write deferred order: %w", err)
	}
	
	log.Printf("Order deferred: %s %s %.8f@%.8f", 
		event.GridID, event.Side, event.Quantity, event.Price)
	
	return nil
}

// processDeferredDequeue 处理延迟订单出队
func (r *Reducer) processDeferredDequeue(event types.Event) error {
	// 从队头弹出延迟订单
	deferredOrder, err := r.storage.PopDeferredOrder()
	if err != nil {
		return fmt.Errorf("failed to pop deferred order: %w", err)
	}
	
	if deferredOrder == nil {
		log.Println("No deferred orders to process")
		return nil
	}
	
	// 创建订单意图事件
	orderIntent := types.Event{
		Timestamp: time.Now().UnixNano(),
		Type:      types.EventOrderIntent,
		GridID:    deferredOrder.GridID,
		Side:      deferredOrder.Side,
		Price:     deferredOrder.Price,
		Quantity:  deferredOrder.Quantity,
	}
	
	// 写入订单意图
	if err := r.storage.WriteEvent(orderIntent); err != nil {
		// 如果写入失败，需要将订单重新放回队列
		if writeErr := r.storage.WriteDeferredOrder(deferredOrder); writeErr != nil {
			log.Printf("Failed to restore deferred order: %v", writeErr)
		}
		return fmt.Errorf("failed to write order intent from deferred: %w", err)
	}
	
	log.Printf("Deferred order processed: %s %s %.8f@%.8f", 
		deferredOrder.GridID, deferredOrder.Side, deferredOrder.Quantity, deferredOrder.Price)
	
	return nil
}

// ProcessDeferredQueue 处理延迟队列（定期调用）
func (r *Reducer) ProcessDeferredQueue() error {
	// 检查当前活跃订单数量
	activeOrders := r.storage.GetOrderTable().GetActiveCount()
	maxActiveOrders := 100 // 可配置的最大活跃订单数
	
	// 如果活跃订单数量低于阈值，处理延迟队列
	if activeOrders < maxActiveOrders {
		availableSlots := maxActiveOrders - activeOrders
		
		for i := 0; i < availableSlots; i++ {
			// 创建出队事件
			dequeueEvent := types.Event{
				Timestamp: time.Now().UnixNano(),
				Type:      types.EventDeferredDequeue,
			}
			
			// 处理出队
			if err := r.processDeferredDequeue(dequeueEvent); err != nil {
				log.Printf("Failed to process deferred dequeue: %v", err)
				break
			}
		}
	}
	
	return nil
}

// WriteOrderIntent 写入订单意图事件
func (r *Reducer) WriteOrderIntent(clientOrderID, symbol string, side types.OrderSide, price, qty float64) error {
	event := types.Event{
		Type:      types.EventOrderIntent,
		OrderID:   clientOrderID,
		Side:      side,
		Price:     price,
		Quantity:  qty,
		Source:    "INTENT", // 标识来源为意图记录
		LatencyMs: 0,        // 意图记录无网络延迟
		Extra: map[string]interface{}{
			"symbol": symbol,
		},
	}
	
	return r.ProcessEvent(event)
}

// WriteOrderRejected 写入订单拒绝事件
func (r *Reducer) WriteOrderRejected(clientOrderID, symbol string, side types.OrderSide, price, qty float64, reason string) error {
	event := types.Event{
		Type:      types.EventOrderRejected,
		OrderID:   clientOrderID,
		Side:      side,
		Price:     price,
		Quantity:  qty,
		Source:    "VALIDATION", // 标识来源为验证拒绝
		LatencyMs: 0,
		Extra: map[string]interface{}{
			"symbol": symbol,
			"reason": reason,
		},
	}
	
	return r.ProcessEvent(event)
}

// WriteOrderPlaced 写入订单下单事件
func (r *Reducer) WriteOrderPlaced(clientOrderID, symbol string, side types.OrderSide, price, qty float64, reduceOnly bool) error {
	event := types.Event{
		Type:       types.EventOrderPlaced,
		OrderID:    clientOrderID,
		Side:       side,
		Price:      price,
		Quantity:   qty,
		ReduceOnly: reduceOnly,
		Source:     "REST", // 标识来源为REST API
		LatencyMs:  0,      // 将在实际下单时更新延迟
		Extra: map[string]interface{}{
			"symbol": symbol,
		},
	}
	
	return r.ProcessEvent(event)
}

// WriteOrderFilled 写入订单成交事件
func (r *Reducer) WriteOrderFilled(clientOrderID string, filledQty, avgPrice float64, source string) error {
	event := types.Event{
		Type:      types.EventOrderFilled,
		OrderID:   clientOrderID,
		Quantity:  filledQty,
		FilledQty: filledQty,
		AvgPrice:  avgPrice,
		Source:    source,    // 保持传入的source
		LatencyMs: 0,         // 将在实际处理时更新延迟
	}
	
	return r.ProcessEvent(event)
}

// WriteOrderCanceled 写入订单取消事件
func (r *Reducer) WriteOrderCanceled(clientOrderID, source string) error {
	event := types.Event{
		Type:      types.EventOrderCanceled,
		OrderID:   clientOrderID,
		Source:    source,    // 保持传入的source
		LatencyMs: 0,         // 将在实际处理时更新延迟
	}
	
	return r.ProcessEvent(event)
}

// 辅助函数
func extractSymbolFromEvent(event types.Event) string {
	if event.Extra != nil {
		if symbol, ok := event.Extra["symbol"].(string); ok {
			return symbol
		}
	}
	return ""
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}


func (r *Reducer) processBuyToSell(event types.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Printf("Processing BuyToSell event: %+v", event)
	
	// 记录买转卖事件
	if err := r.storage.WriteEvent(event); err != nil {
		return fmt.Errorf("failed to write BuyToSell event: %v", err)
	}

	// 更新监控指标
	if r.monitor != nil {
		r.monitor.RecordEvent("buy_to_sell", extractSymbolFromEvent(event))
	}

	return nil
}

func (r *Reducer) processSellToBuy(event types.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Printf("Processing SellToBuy event: %+v", event)
	
	// 记录卖转买事件
	if err := r.storage.WriteEvent(event); err != nil {
		return fmt.Errorf("failed to write SellToBuy event: %v", err)
	}

	// 更新监控指标
	if r.monitor != nil {
		r.monitor.RecordEvent("sell_to_buy", extractSymbolFromEvent(event))
	}

	return nil
}

func (r *Reducer) processPositionUpdate(event types.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Printf("Processing PositionUpdate event: %+v", event)
	
	// 记录持仓更新事件
	if err := r.storage.WriteEvent(event); err != nil {
		return fmt.Errorf("failed to write PositionUpdate event: %v", err)
	}

	// 更新监控指标
	if r.monitor != nil {
		r.monitor.RecordEvent("position_update", extractSymbolFromEvent(event))
	}

	return nil
}

func (r *Reducer) processSystemStateChanged(event types.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Printf("Processing SystemStateChanged event: %+v", event)
	
	// 记录系统状态变更事件
	if err := r.storage.WriteEvent(event); err != nil {
		return fmt.Errorf("failed to write SystemStateChanged event: %v", err)
	}

	// 更新监控指标
	if r.monitor != nil {
		r.monitor.RecordEvent("system_state_changed", extractSymbolFromEvent(event))
	}

	return nil
}
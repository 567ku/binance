package handler

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"binance-grid-trader/binance"
	"binance-grid-trader/grid"
	"binance-grid-trader/storage"
	"binance-grid-trader/types"
)

// EventHandler WebSocket事件处理器
type EventHandler struct {
	gridEngine *grid.Engine
	storage    *storage.Storage
	client     *binance.Client
}

// NewEventHandler 创建事件处理器
func NewEventHandler(gridEngine *grid.Engine, storage *storage.Storage, client *binance.Client) *EventHandler {
	return &EventHandler{
		gridEngine: gridEngine,
		storage:    storage,
		client:     client,
	}
}

// HandleOrderUpdate 处理订单更新事件
func (h *EventHandler) HandleOrderUpdate(data []byte) {
	var event binance.OrderUpdateEvent
	if err := json.Unmarshal(data, &event); err != nil {
		log.Printf("Failed to unmarshal order update event: %v", err)
		return
	}

	log.Printf("Order update: %s %s %s", event.ClientOrderID, event.OrderStatus, event.ExecutionType)

	// 记录事件
	h.recordEvent("ORDER_UPDATE", data)

	// 处理订单状态变化
	switch event.ExecutionType {
	case "NEW":
		h.handleOrderNew(&event)
	case "TRADE":
		h.handleOrderTrade(&event)
	case "CANCELED":
		h.handleOrderCanceled(&event)
	case "REJECTED":
		h.handleOrderRejected(&event)
	case "EXPIRED":
		h.handleOrderExpired(&event)
	}
}

// handleOrderNew 处理新订单
func (h *EventHandler) handleOrderNew(event *binance.OrderUpdateEvent) {
	// 更新网格引擎中的订单状态
	if err := h.gridEngine.UpdateOrderStatus(event.ClientOrderID, types.OrderStatusNew); err != nil {
		log.Printf("Failed to update order status to NEW: %v", err)
		return
	}

	// 记录操作
	operation := &types.Operation{
		ClientOrderID: event.ClientOrderID,
		Operation:     "ORDER_PLACED",
		Status:        "SUCCESS",
		Timestamp:     time.Now(),
		Response: map[string]interface{}{
			"symbol":   event.Symbol,
			"side":     event.Side,
			"quantity": event.OriginalQuantity,
			"price":    event.OriginalPrice,
			"orderID":  strconv.FormatInt(event.OrderID, 10),
		},
	}

	if err := h.storage.WriteEvent(types.Event{
		Type:      types.EventOrderPlaced,
		Extra:     map[string]interface{}{"operation": operation},
		Timestamp: time.Now().Unix(),
	}); err != nil {
		log.Printf("Failed to write operation event: %v", err)
	}
}

// handleOrderTrade 处理订单成交
func (h *EventHandler) handleOrderTrade(event *binance.OrderUpdateEvent) {
	// 解析成交信息
	quantity, err := strconv.ParseFloat(event.LastExecutedQuantity, 64)
	if err != nil {
		log.Printf("Failed to parse executed quantity: %v", err)
		return
	}

	price, err := strconv.ParseFloat(event.LastExecutedPrice, 64)
	if err != nil {
		log.Printf("Failed to parse executed price: %v", err)
		return
	}

	// 创建执行记录
	execution := &types.Execution{
		OrderID: event.ClientOrderID,
		TradeID: strconv.FormatInt(event.TradeID, 10),
		Price:   price,
		Qty:     quantity,
		Time:    time.Unix(event.OrderTradeTime/1000, 0),
	}

	// 记录执行事件
	if err := h.storage.WriteEvent(types.Event{
		Type:      types.EventOrderFilled,
		OrderID:   event.ClientOrderID,
		Side:      types.OrderSide(event.Side),
		Price:     price,
		Quantity:  quantity,
		Source:    "WS",
		Timestamp: time.Now().Unix(),
		Extra:     map[string]interface{}{"execution": execution},
	}); err != nil {
		log.Printf("Failed to write execution event: %v", err)
	}

	// 检查订单是否完全成交
	if event.OrderStatus == "FILLED" {
		// 更新订单状态为已成交
		if err := h.gridEngine.UpdateOrderStatus(event.ClientOrderID, types.OrderStatusFilled); err != nil {
			log.Printf("Failed to update order status to FILLED: %v", err)
			return
		}

		// 通知网格引擎订单已成交
		if err := h.gridEngine.OnOrderFilled(event.ClientOrderID, execution); err != nil {
			log.Printf("Failed to handle order filled: %v", err)
			return
		}

		log.Printf("Order %s filled completely", event.ClientOrderID)
	} else if event.OrderStatus == "PARTIALLY_FILLED" {
		// 更新订单状态为部分成交
		if err := h.gridEngine.UpdateOrderStatus(event.ClientOrderID, types.OrderStatusPartiallyFilled); err != nil {
			log.Printf("Failed to update order status to PARTIALLY_FILLED: %v", err)
		}

		log.Printf("Order %s partially filled", event.ClientOrderID)
	}
}

// handleOrderCanceled 处理订单取消
func (h *EventHandler) handleOrderCanceled(event *binance.OrderUpdateEvent) {
	// 更新网格引擎中的订单状态
	if err := h.gridEngine.UpdateOrderStatus(event.ClientOrderID, types.OrderStatusCanceled); err != nil {
		log.Printf("Failed to update order status to CANCELED: %v", err)
		return
	}

	// 通知网格引擎订单已取消
	if err := h.gridEngine.OnOrderCanceled(event.ClientOrderID); err != nil {
		log.Printf("Failed to handle order canceled: %v", err)
	}

	log.Printf("Order %s canceled", event.ClientOrderID)
}

// handleOrderRejected 处理订单拒绝
func (h *EventHandler) handleOrderRejected(event *binance.OrderUpdateEvent) {
	// 更新网格引擎中的订单状态
	if err := h.gridEngine.UpdateOrderStatus(event.ClientOrderID, types.OrderStatusCanceled); err != nil {
		log.Printf("Failed to update order status to REJECTED: %v", err)
		return
	}

	// 解析价格和数量
	price, _ := strconv.ParseFloat(event.OriginalPrice, 64)
	quantity, _ := strconv.ParseFloat(event.OriginalQuantity, 64)

	// 记录拒绝信息
	rejection := &types.Rejection{
		Timestamp:   time.Now().Unix(),
		OrderID:     event.ClientOrderID,
		Side:        types.OrderSide(event.Side),
		Price:       price,
		Quantity:    quantity,
		Code:        -1001,
		Message:     "ORDER_REJECTED",
		RawResponse: map[string]interface{}{
			"symbol": event.Symbol,
			"reason": "ORDER_REJECTED",
		},
	}

	if err := h.storage.WriteRejection(rejection); err != nil {
		log.Printf("Failed to write rejection: %v", err)
	}

	// 创建拒绝事件
	rejectionEvent := &types.Event{
		Type:      types.EventOrderRejected,
		Timestamp: time.Now().Unix(),
		OrderID:   event.ClientOrderID,
		Side:      types.OrderSide(event.Side),
		Price:     price,
		Quantity:  quantity,
		Source:    "WS",
		Extra: map[string]interface{}{
			"symbol": event.Symbol,
			"reason": "ORDER_REJECTED",
		},
	}

	if err := h.storage.WriteEvent(*rejectionEvent); err != nil {
		log.Printf("Failed to write rejection event: %v", err)
	}

	log.Printf("Order %s rejected", event.ClientOrderID)
}

// handleOrderExpired 处理订单过期
func (h *EventHandler) handleOrderExpired(event *binance.OrderUpdateEvent) {
	// 更新网格引擎中的订单状态
	if err := h.gridEngine.UpdateOrderStatus(event.ClientOrderID, types.OrderStatusExpired); err != nil {
		log.Printf("Failed to update order status to EXPIRED: %v", err)
		return
	}

	// 通知网格引擎订单已过期
	if err := h.gridEngine.OnOrderCanceled(event.ClientOrderID); err != nil {
		log.Printf("Failed to handle order expired: %v", err)
	}

	log.Printf("Order %s expired", event.ClientOrderID)
}

// HandleAccountUpdate 处理账户更新事件
func (h *EventHandler) HandleAccountUpdate(data []byte) {
	var event binance.AccountUpdateEvent
	if err := json.Unmarshal(data, &event); err != nil {
		log.Printf("Failed to unmarshal account update event: %v", err)
		return
	}

	log.Printf("Account update: reason=%s", event.AccountData.Reason)

	// 记录事件
	h.recordEvent("ACCOUNT_UPDATE", data)

	// 处理余额变化
	for _, balance := range event.AccountData.Balances {
		log.Printf("Balance update: %s wallet=%s cross=%s change=%s", 
			balance.Asset, balance.WalletBalance, balance.CrossWalletBalance, balance.BalanceChange)
	}

	// 处理持仓变化
	for _, position := range event.AccountData.Positions {
		log.Printf("Position update: %s amount=%s entry=%s unrealized=%s", 
			position.Symbol, position.PositionAmount, position.EntryPrice, position.UnrealizedPnL)
	}
}

// HandleMarkPrice 处理标记价格事件
func (h *EventHandler) HandleMarkPrice(data []byte) {
	var event binance.MarkPriceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		log.Printf("Failed to unmarshal mark price event: %v", err)
		return
	}

	// 解析价格
	price, err := strconv.ParseFloat(event.MarkPrice, 64)
	if err != nil {
		log.Printf("Failed to parse mark price: %v", err)
		return
	}

	// 更新网格引擎的当前价格
	if err := h.gridEngine.OnPriceUpdate(price); err != nil {
		log.Printf("Failed to update price in grid engine: %v", err)
		return
	}

	// 记录市场数据
	marketData := &types.MarketData{
		Symbol:    event.Symbol,
		Price:     price,
		Timestamp: time.Unix(event.EventTime/1000, 0),
	}

	if err := h.storage.WriteEvent(types.Event{
		Type:      types.EventManualAction,
		Timestamp: time.Now().Unix(),
		Extra:     map[string]interface{}{"market_data": marketData},
	}); err != nil {
		log.Printf("Failed to write market data event: %v", err)
	}
}

// HandleError 处理错误事件
func (h *EventHandler) HandleError(err error) {
	log.Printf("WebSocket error: %v", err)

	// 记录错误事件
	errorEvent := types.Event{
		Type:      types.EventTypeSystemStateChanged,
		Source:    "WS",  // 标识来源为WebSocket
		LatencyMs: 0,     // 错误事件无延迟
		Timestamp: time.Now().Unix(),
		Extra: map[string]interface{}{
			"error":   err.Error(),
			"context": "websocket_handler",
		},
	}

	if writeErr := h.storage.WriteEvent(errorEvent); writeErr != nil {
		log.Printf("Failed to write error event: %v", writeErr)
	}
}

// recordEvent 记录原始事件
func (h *EventHandler) recordEvent(eventType string, data []byte) {
	rawEvent := map[string]interface{}{
		"type": eventType,
		"data": json.RawMessage(data),
	}

	event := &types.Event{
		Type:      types.EventManualAction,
		Source:    "WS",  // 标识来源为WebSocket
		LatencyMs: 0,     // 原始事件记录无延迟
		Timestamp: time.Now().UnixNano(),
		Extra:     map[string]interface{}{"raw_event": rawEvent},
	}

	if err := h.storage.WriteEvent(event); err != nil {
		log.Printf("Failed to write raw event: %v", err)
	}
}

// ProcessPendingOrders 处理待处理订单
func (h *EventHandler) ProcessPendingOrders() error {
	pendingOrders := h.gridEngine.GetPendingOrders()
	
	if len(pendingOrders) == 0 {
		return nil
	}
	
	log.Printf("Processing %d pending orders", len(pendingOrders))
	
	for _, order := range pendingOrders {
		// 检查订单是否已经存在于交易所
		if h.isOrderExistsOnExchange(order.ClientOrderID) {
			log.Printf("Order %s already exists on exchange, skipping", order.ClientOrderID)
			// 更新为已提交状态，等待WebSocket事件更新
			if err := h.gridEngine.UpdateOrderStatus(order.ClientOrderID, types.OrderStatusPartiallyFilled); err != nil {
				log.Printf("Failed to update existing order status: %v", err)
			}
			continue
		}
		
		// 提交订单到交易所
		if err := h.submitOrder(order); err != nil {
			log.Printf("Failed to submit order %s: %v", order.ClientOrderID, err)
			
			// 记录拒绝信息
			rejection := &types.Rejection{
				Timestamp:   time.Now().Unix(),
				OrderID:     order.ClientOrderID,
				Side:        order.Side,
				Price:       order.Price,
				Quantity:    order.OrigQty,
				Code:        -1002,
				Message:     err.Error(),
				RawResponse: map[string]interface{}{
					"symbol": order.Symbol,
					"error":  err.Error(),
				},
			}

			if err := h.storage.WriteRejection(rejection); err != nil {
				log.Printf("Failed to write rejection: %v", err)
			}

			// 更新订单状态为拒绝
			if err := h.gridEngine.UpdateOrderStatus(order.ClientOrderID, types.OrderStatusCanceled); err != nil {
				log.Printf("Failed to update order status to REJECTED: %v", err)
			}
			
			continue
		}

		log.Printf("Order %s submitted successfully", order.ClientOrderID)
	}

	return nil
}

// isOrderExistsOnExchange 检查订单是否已存在于交易所
func (h *EventHandler) isOrderExistsOnExchange(clientOrderID string) bool {
	// 通过查询订单API检查订单是否存在
	req := &binance.QueryOrderRequest{
		Symbol:            h.gridEngine.Config.Symbol,
		OrigClientOrderID: clientOrderID,
	}
	
	_, err := h.client.QueryOrder(req)
	// 如果查询成功，说明订单存在；如果返回错误，可能是订单不存在
	return err == nil
}

// submitOrder 提交订单（带写屏障机制）
func (h *EventHandler) submitOrder(order *types.Order) error {
	// 写屏障：下单前先记录ORDER_INTENT事件
	intentEvent := &types.Event{
		Type:      types.EventOrderIntent,
		OrderID:   order.ClientOrderID,
		Side:      order.Side,
		Price:     order.Price,
		Quantity:  order.OrigQty,
		Source:    "HANDLER",
		Timestamp: time.Now().UnixNano(),
		Extra: map[string]interface{}{
			"symbol":       order.Symbol,
			"positionSide": order.PositionSide,
			"orderType":    order.OrderType,
			"timeInForce":  order.TimeInForce,
			"reduceOnly":   order.ReduceOnly,
		},
	}

	// 同步写入ORDER_INTENT事件（关键事件）
	if err := h.storage.WriteEvent(intentEvent); err != nil {
		log.Printf("Failed to write ORDER_INTENT event for order %s: %v", order.ClientOrderID, err)
		return fmt.Errorf("failed to write order intent: %w", err)
	}

	// 构建下单请求
	request := &binance.PlaceOrderRequest{
		Symbol:           order.Symbol,
		Side:             string(order.Side),
		PositionSide:     string(order.PositionSide),
		Type:             string(order.OrderType),
		Quantity:         order.OrigQty,
		Price:            order.Price,
		TimeInForce:      string(order.TimeInForce),
		ReduceOnly:       order.ReduceOnly,
		NewClientOrderID: order.ClientOrderID,
	}

	// 提交订单到交易所
	_, err := h.client.PlaceOrder(request)
	if err != nil {
		// 下单失败，记录失败事件
		failEvent := &types.Event{
			Type:      types.EventOrderCanceled,
			OrderID:   order.ClientOrderID,
			Source:    "HANDLER",
			Timestamp: time.Now().UnixNano(),
			Extra: map[string]interface{}{
				"error":  err.Error(),
				"reason": "PLACE_ORDER_FAILED",
			},
		}
		
		if writeErr := h.storage.WriteEvent(failEvent); writeErr != nil {
			log.Printf("Failed to write order failure event: %v", writeErr)
		}
		
		return err
	}

	// 下单成功，等待WebSocket事件确认
	log.Printf("Order %s submitted successfully, waiting for exchange confirmation", order.ClientOrderID)
	return nil
}

// GetSystemStatus 获取系统状态
func (h *EventHandler) GetSystemStatus() map[string]interface{} {
	stats := h.gridEngine.GetStatistics()
	
	// 添加处理器状态
	stats["handler_status"] = "ACTIVE"
	stats["last_update"] = time.Now()
	
	return stats
}
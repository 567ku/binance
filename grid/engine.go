package grid

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"binance-grid-trader/types"
	"github.com/google/uuid"
)

// Engine 网格引擎
type Engine struct {
	Config      *types.GridConfig  // 改为公开字段
	levels      []types.GridLevel
	orders      map[string]*types.Order
	orderPairs  map[string]*types.OrderPair
	mu          sync.RWMutex
	currentPrice float64
	isRunning   bool
	isPaused    bool  // 网格是否暂停
	closeOnlyMode bool // 只平仓模式
}

// NewEngine 创建网格引擎
func NewEngine(config *types.GridConfig) *Engine {
	return &Engine{
		Config:     config,
		orders:     make(map[string]*types.Order),
		orderPairs: make(map[string]*types.OrderPair),
	}
}

// Initialize 初始化网格
func (e *Engine) Initialize() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 计算网格层级
	if err := e.calculateGridLevels(); err != nil {
		return fmt.Errorf("failed to calculate grid levels: %w", err)
	}

	return nil
}

// calculateGridLevels 计算网格层级
func (e *Engine) calculateGridLevels() error {
	if e.Config.PriceMax <= e.Config.PriceMin {
		return fmt.Errorf("max price must be greater than min price")
	}

	if e.Config.StepSize <= 0 {
		return fmt.Errorf("step size must be positive")
	}

	// 计算网格数量，顶格不买入，所以网格点不包含最高价
	priceRange := e.Config.PriceMax - e.Config.PriceMin
	gridCount := int(priceRange / e.Config.StepSize)

	e.levels = make([]types.GridLevel, 0, gridCount)

	// 生成网格层级，从最低价开始，不包含最高价
	for i := 0; i < gridCount; i++ {
		price := e.Config.PriceMin + float64(i)*e.Config.StepSize
		
		level := types.GridLevel{
			Price:       price,
			HasPosition: false,
		}

		e.levels = append(e.levels, level)
	}

	// 按价格排序
	sort.Slice(e.levels, func(i, j int) bool {
		return e.levels[i].Price < e.levels[j].Price
	})

	return nil
}

// Start 启动网格交易
func (e *Engine) Start(currentPrice float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.currentPrice = currentPrice
	e.isRunning = true

	// 生成初始订单
	return e.generateInitialOrders()
}

// Stop 停止网格交易
func (e *Engine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isRunning = false
	return nil
}

// Pause 暂停网格交易（停止生成新买单）
func (e *Engine) Pause() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isPaused = true
	return nil
}

// Resume 恢复网格交易
func (e *Engine) Resume() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isPaused = false
	return nil
}

// SetCloseOnlyMode 设置只平仓模式
func (e *Engine) SetCloseOnlyMode(enabled bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closeOnlyMode = enabled
	return nil
}

// IsPaused 检查网格是否暂停
func (e *Engine) IsPaused() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isPaused
}

// IsCloseOnlyMode 检查是否为只平仓模式
func (e *Engine) IsCloseOnlyMode() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.closeOnlyMode
}

// IsRunning 检查网格是否正在运行
func (e *Engine) IsRunning() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isRunning
}

// generateInitialOrders 生成初始订单
func (e *Engine) generateInitialOrders() error {
	// 如果网格暂停或只平仓模式，不生成新买单
	if e.isPaused || e.closeOnlyMode {
		return nil
	}

	buyCount := 0

	// 只生成买单，且只在市价下方
	for i := range e.levels {
		level := &e.levels[i]
		
		// 只在当前价格下方生成买单，且不超过最大买单数
		if level.Price < e.currentPrice && buyCount < e.Config.MaxBuyOrders {
			order := e.createBuyOrder(level)
			e.orders[order.ClientOrderID] = order
			level.BuyOrderID = order.ClientOrderID
			buyCount++
		}
	}

	return nil
}

// createBuyOrder 创建买单
func (e *Engine) createBuyOrder(level *types.GridLevel) *types.Order {
	return &types.Order{
		ClientOrderID: uuid.New().String(),
		Symbol:        e.Config.Symbol,
		Side:          types.OrderSideBuy,
		PositionSide:  types.PositionSideLong,
		OrderType:     types.OrderTypeLimit,
		OrigQty:       e.Config.OrderQty,
		Price:         level.Price,
		TimeInForce:   types.TimeInForceGTC,
		ReduceOnly:    false,
		Status:        types.OrderStatusNew,
		CreatedAt:     time.Now(),
		GridLegID:     level.GridLegID,
	}
}

// createSellOrder 创建卖单
func (e *Engine) createSellOrder(level *types.GridLevel) *types.Order {
	return &types.Order{
		ClientOrderID: uuid.New().String(),
		Symbol:        e.Config.Symbol,
		Side:          types.OrderSideSell,
		PositionSide:  types.PositionSideLong,
		OrderType:     types.OrderTypeLimit,
		OrigQty:       e.Config.OrderQty,
		Price:         level.Price + e.Config.StepSize, // 卖价 = 买价 + 步长
		TimeInForce:   types.TimeInForceGTC,
		ReduceOnly:    true, // 卖单必须设置为只平仓
		Status:        types.OrderStatusNew,
		CreatedAt:     time.Now(),
		GridLegID:     level.GridLegID,
	}
}

// OnOrderFilled 处理订单成交
func (e *Engine) OnOrderFilled(orderID string, execution *types.Execution) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	order, exists := e.orders[orderID]
	if !exists {
		return fmt.Errorf("order not found: %s", orderID)
	}

	// 更新订单状态
	order.Status = types.OrderStatusFilled
	order.ExecutedQty = execution.Qty
	order.UpdatedAt = time.Now()

	// 生成配对订单
	return e.generatePairOrder(order, execution)
}

// generatePairOrder 生成配对订单
func (e *Engine) generatePairOrder(filledOrder *types.Order, execution *types.Execution) error {
	var pairOrder *types.Order

	// 根据成交订单类型生成配对订单
	if filledOrder.Side == types.OrderSideBuy {
		// 买单成交，生成卖单
		pairOrder = e.createPairSellOrder(filledOrder, execution)
	} else {
		// 卖单成交，生成买单
		pairOrder = e.createPairBuyOrder(filledOrder, execution)
	}

	// 保存配对订单
	e.orders[pairOrder.ClientOrderID] = pairOrder

	// 记录配对关系
	orderPair := &types.OrderPair{
		GridLegID:   filledOrder.GridLegID,
		Price:       execution.Price,
		Qty:         execution.Qty,
		MatchedAt:   execution.Time,
	}

	if filledOrder.Side == types.OrderSideBuy {
		orderPair.BuyOrderID = filledOrder.OrderID
		orderPair.SellOrderID = pairOrder.ClientOrderID
	} else {
		orderPair.SellOrderID = filledOrder.OrderID
		orderPair.BuyOrderID = pairOrder.ClientOrderID
	}

	e.orderPairs[orderPair.GridLegID] = orderPair

	return nil
}

// createPairSellOrder 创建配对卖单
func (e *Engine) createPairSellOrder(buyOrder *types.Order, execution *types.Execution) *types.Order {
	sellPrice := buyOrder.Price + e.Config.StepSize
	
	return &types.Order{
		ClientOrderID: uuid.New().String(),
		Symbol:        e.Config.Symbol,
		Side:          types.OrderSideSell,
		PositionSide:  types.PositionSideLong,
		OrderType:     types.OrderTypeLimit,
		OrigQty:       execution.Qty,
		Price:         sellPrice,
		TimeInForce:   types.TimeInForceGTC,
		ReduceOnly:    true, // 配对卖单必须设置为只平仓
		Status:        types.OrderStatusNew,
		CreatedAt:     time.Now(),
		GridLegID:     buyOrder.GridLegID,
	}
}

// createPairBuyOrder 创建配对买单（卖单成交后的回补买单）
func (e *Engine) createPairBuyOrder(sellOrder *types.Order, execution *types.Execution) *types.Order {
	// 在只平仓模式下，不创建新的买单
	if e.closeOnlyMode {
		return nil
	}
	
	// 计算买入价格（卖出价格 - 步长）
	buyPrice := execution.Price - e.Config.StepSize
	
	// 查找对应的网格层级
	var targetLevel *types.GridLevel
	for i := range e.levels {
		if math.Abs(e.levels[i].Price-buyPrice) < 0.01 { // 允许小的浮点误差
			targetLevel = &e.levels[i]
			break
		}
	}
	
	// 如果找不到对应层级，创建一个新的
	if targetLevel == nil {
		newLevel := types.GridLevel{
			Price:     buyPrice,
			GridLegID: sellOrder.GridLegID,
		}
		e.levels = append(e.levels, newLevel)
		targetLevel = &e.levels[len(e.levels)-1]
	}
	
	// 检查同格约束：确保该层级没有活跃的买单
	if err := e.ensureUniqueness(targetLevel, types.OrderSideBuy, buyPrice); err != nil {
		log.Printf("Cannot create pair buy order: %v", err)
		return nil
	}

	buyOrder := &types.Order{
		ClientOrderID: uuid.New().String(),
		Symbol:        e.Config.Symbol,
		Side:          types.OrderSideBuy,
		PositionSide:  types.PositionSideLong,
		OrderType:     types.OrderTypeLimit,
		OrigQty:       execution.Qty,
		Price:         buyPrice,
		TimeInForce:   types.TimeInForceGTC,
		ReduceOnly:    false,
		Status:        types.OrderStatusNew,
		CreatedAt:     time.Now(),
		GridLegID:     sellOrder.GridLegID,
	}
	
	// 更新层级状态
	targetLevel.BuyOrderID = buyOrder.ClientOrderID
	
	return buyOrder
}

// ensureUniqueness 确保同格约束：每个网格层级最多只有1个买单和1个卖单
// 如果发现同格有活跃订单且价格不同，则撤销旧订单（撤旧→新）
func (e *Engine) ensureUniqueness(level *types.GridLevel, side types.OrderSide, newPrice float64) error {
	if side == types.OrderSideBuy {
		// 检查是否已有活跃的买单
		if level.BuyOrderID != "" {
			// 检查该买单是否仍然活跃
			if order, exists := e.orders[level.BuyOrderID]; exists && order.Status.IsActive() {
				// 如果价格相同，不需要重复下单
				if math.Abs(order.Price-newPrice) < 0.01 {
					return fmt.Errorf("duplicate buy order at level %.2f with same price: existing order %s", level.Price, level.BuyOrderID)
				}
				
				// 价格不同，撤销旧订单（撤旧→新）
				log.Printf("Price changed for buy order at level %.2f: %.8f -> %.8f, canceling old order %s", 
					level.Price, order.Price, newPrice, level.BuyOrderID)
				
				if err := e.cancelOrder(level.BuyOrderID); err != nil {
					log.Printf("Failed to cancel old buy order %s: %v", level.BuyOrderID, err)
					return fmt.Errorf("failed to cancel old buy order: %w", err)
				}
				
				// 清除引用，允许创建新订单
				level.BuyOrderID = ""
			} else {
				// 如果订单不活跃，清除引用
				level.BuyOrderID = ""
			}
		}
	} else if side == types.OrderSideSell {
		// 检查是否已有活跃的卖单
		if level.SellOrderID != "" {
			// 检查该卖单是否仍然活跃
			if order, exists := e.orders[level.SellOrderID]; exists && order.Status.IsActive() {
				// 如果价格相同，不需要重复下单
				if math.Abs(order.Price-newPrice) < 0.01 {
					return fmt.Errorf("duplicate sell order at level %.2f with same price: existing order %s", level.Price, level.SellOrderID)
				}
				
				// 价格不同，撤销旧订单（撤旧→新）
				log.Printf("Price changed for sell order at level %.2f: %.8f -> %.8f, canceling old order %s", 
					level.Price, order.Price, newPrice, level.SellOrderID)
				
				if err := e.cancelOrder(level.SellOrderID); err != nil {
					log.Printf("Failed to cancel old sell order %s: %v", level.SellOrderID, err)
					return fmt.Errorf("failed to cancel old sell order: %w", err)
				}
				
				// 清除引用，允许创建新订单
				level.SellOrderID = ""
			} else {
				// 如果订单不活跃，清除引用
				level.SellOrderID = ""
			}
		}
	}
	
	return nil
}

// cancelOrder 撤销订单
func (e *Engine) cancelOrder(clientOrderID string) error {
	order, exists := e.orders[clientOrderID]
	if !exists {
		return fmt.Errorf("order not found: %s", clientOrderID)
	}
	
	// 如果订单已经不是活跃状态，直接返回成功
	if !order.Status.IsActive() {
		return nil
	}
	
	// 这里需要调用实际的撤单API
	// 由于网格引擎不直接持有client，我们先更新本地状态
	// 实际的撤单操作应该由上层调用者处理
	order.Status = types.OrderStatusCanceled
	order.UpdatedAt = time.Now()
	
	log.Printf("Marked order %s as canceled locally", clientOrderID)
	return nil
}

// validateOrderPlacement 验证订单下单前的约束条件
func (e *Engine) validateOrderPlacement(order *types.Order) error {
	// 查找对应的网格层级
	var targetLevel *types.GridLevel
	for i := range e.levels {
		if math.Abs(e.levels[i].Price-order.Price) < 0.01 {
			targetLevel = &e.levels[i]
			break
		}
	}
	
	if targetLevel == nil {
		return fmt.Errorf("no grid level found for price %.2f", order.Price)
	}
	
	// 检查同格约束
	return e.ensureUniqueness(targetLevel, order.Side, order.Price)
}

// OnOrderCanceled 处理订单取消
func (e *Engine) OnOrderCanceled(orderID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	order, exists := e.orders[orderID]
	if !exists {
		return fmt.Errorf("order not found: %s", orderID)
	}

	order.Status = types.OrderStatusCanceled
	order.UpdatedAt = time.Now()

	// 释放网格层级
	for i := range e.levels {
		if e.levels[i].GridLegID == order.GridLegID {
			e.levels[i].HasPosition = false
			break
		}
	}

	return nil
}

// OnPriceUpdate 处理价格更新
func (e *Engine) OnPriceUpdate(price float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.currentPrice = price

	if !e.isRunning {
		return nil
	}

	// 检查是否需要调整订单
	return e.adjustOrders()
}

// adjustOrders 调整订单
func (e *Engine) adjustOrders() error {
	// 如果网格暂停，不调整订单
	if e.isPaused {
		return nil
	}

	buyCount := 0
	sellCount := 0

	// 统计当前活跃订单数量
	for _, order := range e.orders {
		if order.Status == types.OrderStatusNew || order.Status == types.OrderStatusPartiallyFilled {
			if order.Side == types.OrderSideBuy {
				buyCount++
			} else {
				sellCount++
			}
		}
	}

	// 根据当前价格调整订单
	for i := range e.levels {
		level := &e.levels[i]
		
		// 只在非只平仓模式下生成新买单
		if level.Price < e.currentPrice && buyCount < e.Config.MaxBuyOrders && !e.closeOnlyMode {
			// 需要买单但没有
			if level.BuyOrderID == "" {
				order := e.createBuyOrder(level)
				e.orders[order.ClientOrderID] = order
				level.BuyOrderID = order.ClientOrderID
				buyCount++
			}
		} else if level.Price > e.currentPrice && sellCount < e.Config.MaxSellOrders {
			// 需要卖单但没有（卖单在任何模式下都允许，因为是平仓）
			if level.SellOrderID == "" {
				order := e.createSellOrder(level)
				e.orders[order.ClientOrderID] = order
				level.SellOrderID = order.ClientOrderID
				sellCount++
			}
		}
	}

	return nil
}

// GetPendingOrders 获取待处理订单
func (e *Engine) GetPendingOrders() []*types.Order {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var pendingOrders []*types.Order
	for _, order := range e.orders {
		if order.Status == types.OrderStatusNew {
			pendingOrders = append(pendingOrders, order)
		}
	}

	return pendingOrders
}

// GetActiveOrders 获取活跃订单
func (e *Engine) GetActiveOrders() []*types.Order {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var activeOrders []*types.Order
	for _, order := range e.orders {
		if order.Status.IsActive() {
			activeOrders = append(activeOrders, order)
		}
	}

	return activeOrders
}

// GetOrderByID 根据ID获取订单
func (e *Engine) GetOrderByID(orderID string) (*types.Order, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	order, exists := e.orders[orderID]
	return order, exists
}

// UpdateOrderStatus 更新订单状态
func (e *Engine) UpdateOrderStatus(orderID string, status types.OrderStatus) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	order, exists := e.orders[orderID]
	if !exists {
		return fmt.Errorf("order not found: %s", orderID)
	}

	order.Status = status
	order.UpdatedAt = time.Now()

	return nil
}

// GetStatistics 获取统计信息
func (e *Engine) GetStatistics() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := make(map[string]interface{})

	// 统计订单数量
	totalOrders := len(e.orders)
	activeOrders := 0
	filledOrders := 0
	canceledOrders := 0

	for _, order := range e.orders {
		switch order.Status {
		case types.OrderStatusNew, types.OrderStatusPartiallyFilled:
			activeOrders++
		case types.OrderStatusFilled:
			filledOrders++
		case types.OrderStatusCanceled, types.OrderStatusExpired:
			canceledOrders++
		}
	}

	stats["total_orders"] = totalOrders
	stats["active_orders"] = activeOrders
	stats["filled_orders"] = filledOrders
	stats["canceled_orders"] = canceledOrders
	stats["total_pairs"] = len(e.orderPairs)
	stats["current_price"] = e.currentPrice
	stats["is_running"] = e.isRunning

	return stats
}

// ValidatePrice 验证价格精度
func (e *Engine) ValidatePrice(price float64) float64 {
	// 根据交易对精度调整价格
	precision := 2 // ETHUSDC 价格精度为2位小数
	multiplier := math.Pow(10, float64(precision))
	return math.Round(price*multiplier) / multiplier
}

// ValidateQuantity 验证数量精度
func (e *Engine) ValidateQuantity(quantity float64) float64 {
	// 根据交易对精度调整数量
	precision := 3 // ETH 数量精度为3位小数
	multiplier := math.Pow(10, float64(precision))
	return math.Round(quantity*multiplier) / multiplier
}

// FormatPrice 格式化价格
func (e *Engine) FormatPrice(price float64) string {
	return strconv.FormatFloat(e.ValidatePrice(price), 'f', 2, 64)
}

// FormatQuantity 格式化数量
func (e *Engine) FormatQuantity(quantity float64) string {
	return strconv.FormatFloat(e.ValidateQuantity(quantity), 'f', 3, 64)
}

// ProcessOrderUpdate 处理订单更新
func (e *Engine) ProcessOrderUpdate(order *types.Order) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	// 更新订单状态
	if existingOrder, exists := e.orders[order.ClientOrderID]; exists {
		existingOrder.Status = order.Status
		existingOrder.ExecutedQty = order.ExecutedQty
		existingOrder.UpdatedAt = order.UpdatedAt
	} else {
		e.orders[order.ClientOrderID] = order
	}
	
	return nil
}

// SetGridIDPrefix 设置网格ID前缀
func (e *Engine) SetGridIDPrefix(prefix string) {
	// 这个方法用于设置网格ID前缀，暂时为空实现
}
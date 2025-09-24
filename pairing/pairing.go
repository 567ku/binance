package pairing

import (
	"fmt"
	"log"
	"sync"
	"time"

	"binance-grid-trader/types"
)

// PairingEngine 订单配对引擎
type PairingEngine struct {
	mu                sync.RWMutex
	gridLegIDGen      *types.GridLegIDGenerator
	pairTable         *types.PairTable
	orderTable        *types.OrderTable
	executionTable    *types.ExecutionTable
	
	// 配对回调
	onPairCreated     func(*types.OrderPair)
	onPairCompleted   func(*types.OrderPair)
	onPartialFill     func(string, *types.Execution) // clientOrderID, execution
}

// NewPairingEngine 创建配对引擎
func NewPairingEngine(
	pairTable *types.PairTable,
	orderTable *types.OrderTable,
	executionTable *types.ExecutionTable,
) *PairingEngine {
	return &PairingEngine{
		gridLegIDGen:   types.NewGridLegIDGenerator(),
		pairTable:      pairTable,
		orderTable:     orderTable,
		executionTable: executionTable,
	}
}

// SetCallbacks 设置回调函数
func (pe *PairingEngine) SetCallbacks(
	onPairCreated func(*types.OrderPair),
	onPairCompleted func(*types.OrderPair),
	onPartialFill func(string, *types.Execution),
) {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	
	pe.onPairCreated = onPairCreated
	pe.onPairCompleted = onPairCompleted
	pe.onPartialFill = onPartialFill
}

// CreateGridLegID 创建网格腿ID
func (pe *PairingEngine) CreateGridLegID(symbol string, price float64) string {
	return pe.gridLegIDGen.Generate(symbol, price)
}

// ProcessOrderUpdate 处理订单更新
func (pe *PairingEngine) ProcessOrderUpdate(order *types.Order) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	
	// 更新订单表
	pe.orderTable.Update(order)
	
	// 如果订单有GridLegID，处理配对逻辑
	if order.GridLegID != "" {
		return pe.processPairing(order)
	}
	
	return nil
}

// ProcessExecution 处理成交记录
func (pe *PairingEngine) ProcessExecution(clientOrderID string, execution *types.Execution) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	
	// 添加成交记录
	pe.executionTable.Add(clientOrderID, execution)
	
	// 获取订单信息
	order, exists := pe.orderTable.Get(clientOrderID)
	if !exists {
		return fmt.Errorf("order not found: %s", clientOrderID)
	}
	
	// 触发部分成交回调
	if pe.onPartialFill != nil {
		pe.onPartialFill(clientOrderID, execution)
	}
	
	// 如果订单有GridLegID，更新配对状态
	if order.GridLegID != "" {
		return pe.updatePairExecution(order, execution)
	}
	
	return nil
}

// processPairing 处理配对逻辑
func (pe *PairingEngine) processPairing(order *types.Order) error {
	// 检查是否已存在配对
	pair, exists := pe.pairTable.Get(order.GridLegID)
	
	if !exists {
		// 创建新配对
		pair = &types.OrderPair{
			GridLegID:   order.GridLegID,
			Price:       order.Price,
			Qty:         order.OrigQty,
			MatchedAt:   time.Now(),
		}
		
		// 根据订单方向设置配对信息
		if order.Side == types.OrderSideBuy {
			pair.BuyOrderID = order.ClientOrderID
		} else {
			pair.SellOrderID = order.ClientOrderID
		}
		
		pe.pairTable.Add(pair)
		
		log.Printf("Created new pair for GridLegID: %s, Side: %s", order.GridLegID, order.Side)
		
		// 触发配对创建回调
		if pe.onPairCreated != nil {
			pe.onPairCreated(pair)
		}
		
	} else {
		// 更新现有配对
		if order.Side == types.OrderSideBuy && pair.BuyOrderID == "" {
			pair.BuyOrderID = order.ClientOrderID
		} else if order.Side == types.OrderSideSell && pair.SellOrderID == "" {
			pair.SellOrderID = order.ClientOrderID
		}
		
		pe.pairTable.Update(pair)
		
		log.Printf("Updated pair for GridLegID: %s, Side: %s", order.GridLegID, order.Side)
	}
	
	// 检查配对是否完成
	return pe.checkPairCompletion(pair)
}

// updatePairExecution 更新配对的成交信息
func (pe *PairingEngine) updatePairExecution(order *types.Order, execution *types.Execution) error {
	pair, exists := pe.pairTable.Get(order.GridLegID)
	if !exists {
		return fmt.Errorf("pair not found for GridLegID: %s", order.GridLegID)
	}
	
	// 更新成交ID
	if order.Side == types.OrderSideBuy {
		pair.BuyTradeID = execution.TradeID
	} else {
		pair.SellTradeID = execution.TradeID
	}
	
	pe.pairTable.Update(pair)
	
	log.Printf("Updated pair execution for GridLegID: %s, Side: %s, TradeID: %s", 
		order.GridLegID, order.Side, execution.TradeID)
	
	// 检查配对是否完成
	return pe.checkPairCompletion(pair)
}

// checkPairCompletion 检查配对是否完成
func (pe *PairingEngine) checkPairCompletion(pair *types.OrderPair) error {
	// 检查是否买卖双方都有订单和成交
	if pair.BuyOrderID != "" && pair.SellOrderID != "" &&
		pair.BuyTradeID != "" && pair.SellTradeID != "" {
		
		// 计算盈亏
		if err := pe.calculateProfitLoss(pair); err != nil {
			log.Printf("Failed to calculate profit/loss for pair %s: %v", pair.GridLegID, err)
		}
		
		log.Printf("Pair completed: GridLegID=%s, P&L=%.6f", pair.GridLegID, pair.ProfitLoss)
		
		// 触发配对完成回调
		if pe.onPairCompleted != nil {
			pe.onPairCompleted(pair)
		}
	}
	
	return nil
}

// calculateProfitLoss 计算配对的盈亏
func (pe *PairingEngine) calculateProfitLoss(pair *types.OrderPair) error {
	// 获取买单成交记录
	buyExecutions := pe.executionTable.Get(pair.BuyOrderID)
	sellExecutions := pe.executionTable.Get(pair.SellOrderID)
	
	if len(buyExecutions) == 0 || len(sellExecutions) == 0 {
		return fmt.Errorf("missing execution data for pair %s", pair.GridLegID)
	}
	
	// 计算买入成本和卖出收入
	var buyTotalCost, sellTotalRevenue float64
	var buyTotalQty, sellTotalQty float64
	
	for _, exec := range buyExecutions {
		buyTotalCost += exec.Price * exec.Qty
		buyTotalQty += exec.Qty
	}
	
	for _, exec := range sellExecutions {
		sellTotalRevenue += exec.Price * exec.Qty
		sellTotalQty += exec.Qty
	}
	
	// 使用较小的数量计算盈亏（处理部分成交情况）
	matchedQty := buyTotalQty
	if sellTotalQty < matchedQty {
		matchedQty = sellTotalQty
	}
	
	if matchedQty > 0 {
		avgBuyPrice := buyTotalCost / buyTotalQty
		avgSellPrice := sellTotalRevenue / sellTotalQty
		
		pair.ProfitLoss = (avgSellPrice - avgBuyPrice) * matchedQty
		pair.Qty = matchedQty
	}
	
	return nil
}

// GetActivePairs 获取活跃配对
func (pe *PairingEngine) GetActivePairs() []*types.OrderPair {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	
	return pe.pairTable.GetActivePairs()
}

// GetCompletedPairs 获取已完成配对
func (pe *PairingEngine) GetCompletedPairs() []*types.OrderPair {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	
	allPairs := pe.pairTable.GetAll()
	var completedPairs []*types.OrderPair
	
	for _, pair := range allPairs {
		if pair.BuyTradeID != "" && pair.SellTradeID != "" {
			completedPairs = append(completedPairs, pair)
		}
	}
	
	return completedPairs
}

// GetPairByGridLegID 根据GridLegID获取配对
func (pe *PairingEngine) GetPairByGridLegID(gridLegID string) (*types.OrderPair, bool) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	
	return pe.pairTable.Get(gridLegID)
}

// RemovePair 移除配对
func (pe *PairingEngine) RemovePair(gridLegID string) {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	
	pe.pairTable.Remove(gridLegID)
	log.Printf("Removed pair: %s", gridLegID)
}

// GetPairStats 获取配对统计信息
func (pe *PairingEngine) GetPairStats() map[string]interface{} {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	
	allPairs := pe.pairTable.GetAll()
	activePairs := pe.pairTable.GetActivePairs()
	
	var totalProfitLoss float64
	completedCount := 0
	
	for _, pair := range allPairs {
		if pair.BuyTradeID != "" && pair.SellTradeID != "" {
			totalProfitLoss += pair.ProfitLoss
			completedCount++
		}
	}
	
	return map[string]interface{}{
		"total_pairs":     len(allPairs),
		"active_pairs":    len(activePairs),
		"completed_pairs": completedCount,
		"total_pnl":       totalProfitLoss,
	}
}

// ProcessPartialFill 处理部分成交
func (pe *PairingEngine) ProcessPartialFill(order *types.Order, execution *types.Execution) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	
	// 更新订单的执行数量
	order.ExecutedQty += execution.Qty
	pe.orderTable.Update(order)
	
	// 添加成交记录
	pe.executionTable.Add(order.ClientOrderID, execution)
	
	log.Printf("Processed partial fill: OrderID=%s, ExecutedQty=%.6f/%.6f", 
		order.ClientOrderID, order.ExecutedQty, order.OrigQty)
	
	// 如果有GridLegID，更新配对信息
	if order.GridLegID != "" {
		return pe.updatePairExecution(order, execution)
	}
	
	return nil
}

// ValidatePairIntegrity 验证配对完整性
func (pe *PairingEngine) ValidatePairIntegrity() []string {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	
	var issues []string
	allPairs := pe.pairTable.GetAll()
	
	for gridLegID, pair := range allPairs {
		// 检查买单是否存在
		if pair.BuyOrderID != "" {
			if _, exists := pe.orderTable.Get(pair.BuyOrderID); !exists {
				issues = append(issues, fmt.Sprintf("Pair %s: buy order %s not found", gridLegID, pair.BuyOrderID))
			}
		}
		
		// 检查卖单是否存在
		if pair.SellOrderID != "" {
			if _, exists := pe.orderTable.Get(pair.SellOrderID); !exists {
				issues = append(issues, fmt.Sprintf("Pair %s: sell order %s not found", gridLegID, pair.SellOrderID))
			}
		}
		
		// 检查成交记录一致性
		if pair.BuyTradeID != "" {
			executions := pe.executionTable.Get(pair.BuyOrderID)
			found := false
			for _, exec := range executions {
				if exec.TradeID == pair.BuyTradeID {
					found = true
					break
				}
			}
			if !found {
				issues = append(issues, fmt.Sprintf("Pair %s: buy trade %s not found in executions", gridLegID, pair.BuyTradeID))
			}
		}
		
		if pair.SellTradeID != "" {
			executions := pe.executionTable.Get(pair.SellOrderID)
			found := false
			for _, exec := range executions {
				if exec.TradeID == pair.SellTradeID {
					found = true
					break
				}
			}
			if !found {
				issues = append(issues, fmt.Sprintf("Pair %s: sell trade %s not found in executions", gridLegID, pair.SellTradeID))
			}
		}
	}
	
	return issues
}
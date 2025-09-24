package monitoring

import (
	"log"
	"time"
)

// DebounceConfig 防抖配置
type DebounceConfig struct {
	// 基于百分比的阈值（0.01 = 1%）
	PercentageThreshold float64
	// 绝对值阈值（作为最小变化量）
	AbsoluteThreshold   float64
	// 时间窗口（在此时间内的更新会被合并）
	TimeWindow          time.Duration
	// 最大延迟时间（超过此时间必须更新）
	MaxDelay            time.Duration
}

// PendingUpdate 待处理的更新
type PendingUpdate struct {
	Asset     string
	Balance   float64
	FirstTime time.Time
	LastTime  time.Time
}

// UpdateBalance 更新余额
func (bm *BalanceMonitor) UpdateBalance(asset string, balance float64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	oldBalance := bm.balances[asset]
	change := balance - oldBalance
	
	// 检查是否需要防抖
	if bm.shouldDebounce(asset, balance, change) {
		log.Printf("[DEBOUNCE] Balance update for %s debounced: %.6f -> %.6f (change: %.6f, %.2f%%)", 
			asset, oldBalance, balance, change, (change/oldBalance)*100)
		bm.addPendingUpdate(asset, balance)
		return
	}
	
	// 立即更新余额
	log.Printf("[BALANCE] Balance updated for %s: %.6f -> %.6f (change: %.6f, %.2f%%)", 
		asset, oldBalance, balance, change, (change/oldBalance)*100)
	bm.performUpdate(asset, balance, change)
}

// shouldDebounce 检查是否应该防抖
func (bm *BalanceMonitor) shouldDebounce(asset string, newBalance, change float64) bool {
	config := bm.getDebounceConfig(asset)
	
	// 获取当前余额
	oldBalance, exists := bm.balances[asset]
	if !exists {
		return false // 新资产不防抖
	}
	
	// 计算变化百分比
	var changePercent float64
	if oldBalance != 0 {
		changePercent = abs(change) / abs(oldBalance)
	}
	
	// 检查是否超过阈值
	if abs(change) >= config.AbsoluteThreshold || changePercent >= config.PercentageThreshold {
		return false // 变化足够大，不防抖
	}
	
	// 检查是否有待处理的更新
	if pending, exists := bm.pendingUpdates[asset]; exists {
		// 检查是否超过最大延迟时间
		if time.Since(pending.FirstTime) >= config.MaxDelay {
			return false // 超过最大延迟，必须更新
		}
	}
	
	return true // 需要防抖
}

// addPendingUpdate 添加待处理更新
func (bm *BalanceMonitor) addPendingUpdate(asset string, balance float64) {
	now := time.Now()
	
	if pending, exists := bm.pendingUpdates[asset]; exists {
		// 更新现有的待处理更新
		pending.Balance = balance
		pending.LastTime = now
		bm.pendingUpdates[asset] = pending
	} else {
		// 创建新的待处理更新
		bm.pendingUpdates[asset] = &PendingUpdate{
			Asset:     asset,
			Balance:   balance,
			FirstTime: now,
			LastTime:  now,
		}
	}
}

// performUpdate 执行实际的余额更新
func (bm *BalanceMonitor) performUpdate(asset string, balance, change float64) {
	bm.balances[asset] = balance
	
	// 记录余额历史
	record := BalanceRecord{
		Timestamp: time.Now(),
		Balance:   balance,
		Change:    change,
	}
	
	if bm.balanceHistory[asset] == nil {
		bm.balanceHistory[asset] = make([]BalanceRecord, 0, 100)
	}
	
	bm.balanceHistory[asset] = append(bm.balanceHistory[asset], record)
	
	// 保持历史记录在合理范围内（最多保留100条）
	if len(bm.balanceHistory[asset]) > 100 {
		bm.balanceHistory[asset] = bm.balanceHistory[asset][1:]
	}
	
	bm.lastUpdate = time.Now()
}

// getDebounceConfig 获取资产的防抖配置
func (bm *BalanceMonitor) getDebounceConfig(asset string) *DebounceConfig {
	if config, exists := bm.debounceConfigs[asset]; exists {
		return config
	}
	
	// 返回默认配置
	return bm.defaultDebounceConfig
}

// SetDebounceConfig 设置特定资产的防抖配置
func (bm *BalanceMonitor) SetDebounceConfig(asset string, config *DebounceConfig) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	bm.debounceConfigs[asset] = config
}

// SetDefaultDebounceConfig 设置默认防抖配置
func (bm *BalanceMonitor) SetDefaultDebounceConfig(config *DebounceConfig) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	bm.defaultDebounceConfig = config
}

// ProcessPendingUpdates 处理待处理的更新
func (bm *BalanceMonitor) ProcessPendingUpdates() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	now := time.Now()
	
	for asset, pending := range bm.pendingUpdates {
		config := bm.getDebounceConfig(asset)
		
		// 检查是否应该处理这个更新
		shouldProcess := false
		
		// 超过最大延迟时间
		if now.Sub(pending.FirstTime) >= config.MaxDelay {
			shouldProcess = true
		}
		
		// 超过时间窗口
		if now.Sub(pending.LastTime) >= config.TimeWindow {
			shouldProcess = true
		}
		
		if shouldProcess {
			oldBalance := bm.balances[asset]
			change := pending.Balance - oldBalance
			log.Printf("[PENDING] Processing pending balance update for %s: %.6f", asset, pending.Balance)
			bm.performUpdate(asset, pending.Balance, change)
			delete(bm.pendingUpdates, asset)
		}
	}
}

// GetBalance 获取余额
func (bm *BalanceMonitor) GetBalance(asset string) (float64, bool) {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	balance, exists := bm.balances[asset]
	return balance, exists
}

// GetAllBalances 获取所有余额
func (bm *BalanceMonitor) GetAllBalances() map[string]float64 {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	result := make(map[string]float64)
	for asset, balance := range bm.balances {
		result[asset] = balance
	}
	
	return result
}

// IsLowBalance 检查是否为低余额
func (bm *BalanceMonitor) IsLowBalance(asset string) bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	balance, exists := bm.balances[asset]
	if !exists {
		return false
	}
	
	threshold, hasThreshold := bm.lowBalanceThresholds[asset]
	if !hasThreshold {
		return false
	}
	
	return balance < threshold
}

// GetLowBalanceAssets 获取低余额资产列表
func (bm *BalanceMonitor) GetLowBalanceAssets() []string {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	var lowBalanceAssets []string
	
	for asset, threshold := range bm.lowBalanceThresholds {
		if balance, exists := bm.balances[asset]; exists && balance < threshold {
			lowBalanceAssets = append(lowBalanceAssets, asset)
		}
	}
	
	return lowBalanceAssets
}

// GetBalanceChange 获取余额变化
func (bm *BalanceMonitor) GetBalanceChange(asset string, duration time.Duration) float64 {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	history, exists := bm.balanceHistory[asset]
	if !exists || len(history) == 0 {
		return 0
	}
	
	cutoff := time.Now().Add(-duration)
	var totalChange float64
	
	for _, record := range history {
		if record.Timestamp.After(cutoff) {
			totalChange += record.Change
		}
	}
	
	return totalChange
}

// GetBalanceHistory 获取余额历史
func (bm *BalanceMonitor) GetBalanceHistory(asset string, limit int) []BalanceRecord {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	history, exists := bm.balanceHistory[asset]
	if !exists {
		return nil
	}
	
	if limit <= 0 || limit > len(history) {
		limit = len(history)
	}
	
	// 返回最近的记录
	start := len(history) - limit
	result := make([]BalanceRecord, limit)
	copy(result, history[start:])
	
	return result
}

// SetLowBalanceThreshold 设置低余额阈值
func (bm *BalanceMonitor) SetLowBalanceThreshold(asset string, threshold float64) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	bm.lowBalanceThresholds[asset] = threshold
}

// GetStats 获取统计信息
func (bm *BalanceMonitor) GetStats() map[string]interface{} {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	stats := map[string]interface{}{
		"balances":              bm.GetAllBalances(),
		"low_balance_assets":    bm.GetLowBalanceAssets(),
		"low_balance_thresholds": bm.lowBalanceThresholds,
		"last_update":           bm.lastUpdate,
	}
	
	// 添加余额变化统计
	balanceChanges := make(map[string]interface{})
	for asset := range bm.balances {
		balanceChanges[asset] = map[string]interface{}{
			"1h_change":  bm.GetBalanceChange(asset, time.Hour),
			"24h_change": bm.GetBalanceChange(asset, 24*time.Hour),
		}
	}
	stats["balance_changes"] = balanceChanges
	
	return stats
}

// HasSignificantBalanceChange 检查是否有显著余额变化
func (bm *BalanceMonitor) HasSignificantBalanceChange(asset string, threshold float64, duration time.Duration) bool {
	change := bm.GetBalanceChange(asset, duration)
	return abs(change) > threshold
}

// GetTotalValue 获取总价值（需要价格信息）
func (bm *BalanceMonitor) GetTotalValue(prices map[string]float64, baseAsset string) float64 {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	var totalValue float64
	
	for asset, balance := range bm.balances {
		if asset == baseAsset {
			totalValue += balance
		} else if price, exists := prices[asset+baseAsset]; exists {
			totalValue += balance * price
		}
	}
	
	return totalValue
}

// Reset 重置所有统计
func (bm *BalanceMonitor) Reset() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	bm.balances = make(map[string]float64)
	bm.balanceHistory = make(map[string][]BalanceRecord)
	bm.lastUpdate = time.Now()
}

// abs 计算绝对值
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
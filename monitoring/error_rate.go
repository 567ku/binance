package monitoring

import (
	"time"
	"binance-grid-trader/types"
)

// RecordRequest 记录请求
func (erm *ErrorRateMonitor) RecordRequest(success bool, errorCode types.ErrorCode) {
	erm.mu.Lock()
	defer erm.mu.Unlock()
	
	// 检查是否需要重置窗口
	if time.Since(erm.windowStart) > erm.windowDuration {
		erm.resetWindow()
	}
	
	erm.totalRequests++
	
	if success {
		erm.successRequests++
	} else {
		erm.errorRequests++
		if errorCode != "" {
			erm.errorsByType[errorCode]++
		}
	}
}

// GetErrorRate 获取错误率
func (erm *ErrorRateMonitor) GetErrorRate() float64 {
	erm.mu.RLock()
	defer erm.mu.RUnlock()
	
	if erm.totalRequests == 0 {
		return 0.0
	}
	
	return float64(erm.errorRequests) / float64(erm.totalRequests)
}

// GetSuccessRate 获取成功率
func (erm *ErrorRateMonitor) GetSuccessRate() float64 {
	erm.mu.RLock()
	defer erm.mu.RUnlock()
	
	if erm.totalRequests == 0 {
		return 1.0 // 没有请求时认为成功率100%
	}
	
	return float64(erm.successRequests) / float64(erm.totalRequests)
}

// GetErrorsByType 获取按类型分组的错误统计
func (erm *ErrorRateMonitor) GetErrorsByType() map[types.ErrorCode]int64 {
	erm.mu.RLock()
	defer erm.mu.RUnlock()
	
	result := make(map[types.ErrorCode]int64)
	for errorCode, count := range erm.errorsByType {
		result[errorCode] = count
	}
	
	return result
}

// GetStats 获取统计信息
func (erm *ErrorRateMonitor) GetStats() map[string]interface{} {
	erm.mu.RLock()
	defer erm.mu.RUnlock()
	
	stats := map[string]interface{}{
		"total_requests":   erm.totalRequests,
		"success_requests": erm.successRequests,
		"error_requests":   erm.errorRequests,
		"error_rate":       erm.GetErrorRate(),
		"success_rate":     erm.GetSuccessRate(),
		"errors_by_type":   erm.GetErrorsByType(),
		"window_start":     erm.windowStart,
		"window_duration":  erm.windowDuration,
	}
	
	return stats
}

// resetWindow 重置统计窗口
func (erm *ErrorRateMonitor) resetWindow() {
	erm.totalRequests = 0
	erm.successRequests = 0
	erm.errorRequests = 0
	erm.errorsByType = make(map[types.ErrorCode]int64)
	erm.windowStart = time.Now()
}

// IsErrorRateHigh 检查错误率是否过高
func (erm *ErrorRateMonitor) IsErrorRateHigh(threshold float64) bool {
	return erm.GetErrorRate() > threshold
}

// GetMostFrequentError 获取最频繁的错误类型
func (erm *ErrorRateMonitor) GetMostFrequentError() (types.ErrorCode, int64) {
	erm.mu.RLock()
	defer erm.mu.RUnlock()
	
	var maxErrorCode types.ErrorCode
	var maxCount int64
	
	for errorCode, count := range erm.errorsByType {
		if count > maxCount {
			maxCount = count
			maxErrorCode = errorCode
		}
	}
	
	return maxErrorCode, maxCount
}

// Reset 重置所有统计
func (erm *ErrorRateMonitor) Reset() {
	erm.mu.Lock()
	defer erm.mu.Unlock()
	
	erm.resetWindow()
}
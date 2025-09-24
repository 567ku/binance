package monitoring

import (
	"fmt"
	"log"
	"time"

	"binance-grid-trader/types"
)

// SetCallback 设置告警回调
func (an *AlertNotifier) SetCallback(callback func(*Alert)) {
	an.mu.Lock()
	defer an.mu.Unlock()
	
	an.onAlert = callback
}

// SendAlert 发送告警
func (an *AlertNotifier) SendAlert(alert *Alert) {
	an.mu.Lock()
	defer an.mu.Unlock()
	
	// 检查告警冷却时间 - 使用配置的冷却时间
	cooldownDuration := 5 * time.Minute // 默认5分钟
	if an.alertConfig != nil {
		cooldownDuration = an.alertConfig.AlertCooldown
	}
	
	if lastTime, exists := an.lastAlerts[alert.Type]; exists {
		if time.Since(lastTime) < cooldownDuration {
			return // 跳过重复告警
		}
	}
	
	// 记录告警时间
	an.lastAlerts[alert.Type] = time.Now()
	
	// 发送告警
	if an.onAlert != nil {
		an.onAlert(alert)
	} else {
		// 默认输出到日志
		log.Printf("ALERT [%s] %s: %s", alert.Level, alert.Title, alert.Message)
	}
}

// checkErrorRate 检查错误率
func (m *Monitor) checkErrorRate() {
	errorRate := m.errorRateMonitor.GetErrorRate()
	
	if errorRate > m.alertConfig.ErrorRateThreshold {
		mostFrequentError, errorCount := m.errorRateMonitor.GetMostFrequentError()
		
		alert := &Alert{
			Type:      "error_rate_high",
			Level:     "ERROR",
			Title:     "错误率过高",
			Message:   fmt.Sprintf("当前错误率: %.2f%%, 阈值: %.2f%%, 最频繁错误: %s (%d次)", 
				errorRate*100, m.alertConfig.ErrorRateThreshold*100, mostFrequentError, errorCount),
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"error_rate":           errorRate,
				"threshold":            m.alertConfig.ErrorRateThreshold,
				"most_frequent_error":  mostFrequentError,
				"error_count":          errorCount,
				"total_requests":       m.errorRateMonitor.totalRequests,
			},
		}
		
		m.alertNotifier.SendAlert(alert)
	}
}

// checkLatency 检查延迟
func (m *Monitor) checkLatency() {
	avgLatency := m.latencyMonitor.GetAverageLatency()
	p95Latency := m.latencyMonitor.GetP95Latency()
	
	if avgLatency > m.alertConfig.LatencyThreshold || p95Latency > m.alertConfig.LatencyThreshold*2 {
		alert := &Alert{
			Type:      "latency_high",
			Level:     "WARN",
			Title:     "API延迟过高",
			Message:   fmt.Sprintf("平均延迟: %dms, P95延迟: %dms, 阈值: %dms", 
				avgLatency.Milliseconds(), p95Latency.Milliseconds(), m.alertConfig.LatencyThreshold.Milliseconds()),
			Timestamp: time.Now(),
			Data: map[string]interface{}{
				"avg_latency_ms": avgLatency.Milliseconds(),
				"p95_latency_ms": p95Latency.Milliseconds(),
				"p99_latency_ms": m.latencyMonitor.GetP99Latency().Milliseconds(),
				"max_latency_ms": m.latencyMonitor.GetMaxLatency().Milliseconds(),
				"threshold_ms":   m.alertConfig.LatencyThreshold.Milliseconds(),
			},
		}
		
		m.alertNotifier.SendAlert(alert)
	}
}

// checkBalance 检查余额
func (m *Monitor) checkBalance() {
	// 检查低余额
	lowBalanceAssets := m.balanceMonitor.GetLowBalanceAssets()
	if len(lowBalanceAssets) > 0 {
		for _, asset := range lowBalanceAssets {
			balance, _ := m.balanceMonitor.GetBalance(asset)
			threshold := m.alertConfig.LowBalanceThresholds[asset]
			
			alert := &Alert{
				Type:      "low_balance",
				Level:     "WARN",
				Title:     "余额不足告警",
				Message:   fmt.Sprintf("资产 %s 余额不足: %.6f, 阈值: %.6f", asset, balance, threshold),
				Timestamp: time.Now(),
				Data: map[string]interface{}{
					"asset":     asset,
					"balance":   balance,
					"threshold": threshold,
				},
			}
			
			m.alertNotifier.SendAlert(alert)
		}
	}
	
	// 检查余额异常变化
	for asset := range m.balanceMonitor.balances {
		if m.balanceMonitor.HasSignificantBalanceChange(asset, m.alertConfig.BalanceChangeThreshold, time.Hour) {
			change := m.balanceMonitor.GetBalanceChange(asset, time.Hour)
			balance, _ := m.balanceMonitor.GetBalance(asset)
			
			level := "INFO"
			if abs(change) > m.alertConfig.BalanceChangeThreshold*2 {
				level = "WARN"
			}
			
			alert := &Alert{
				Type:      "balance_change",
				Level:     level,
				Title:     "余额异常变化",
				Message:   fmt.Sprintf("资产 %s 1小时内变化: %.6f, 当前余额: %.6f", asset, change, balance),
				Timestamp: time.Now(),
				Data: map[string]interface{}{
					"asset":          asset,
					"change":         change,
					"current_balance": balance,
					"threshold":      m.alertConfig.BalanceChangeThreshold,
				},
			}
			
			m.alertNotifier.SendAlert(alert)
		}
	}
}

// CreateErrorAlert 创建错误告警
func CreateErrorAlert(errorCode types.ErrorCode, message string, data map[string]interface{}) *Alert {
	level := "ERROR"
	title := "交易错误"
	
	switch errorCode {
	case types.ErrorCodeRateLimit:
		level = "WARN"
		title = "速率限制"
	case types.ErrorCodeInsufficientBalance:
		level = "WARN"
		title = "余额不足"
	case types.ErrorCodeNetworkError:
		level = "ERROR"
		title = "网络错误"
	case types.ErrorCodeOrderNotFound:
		level = "INFO"
		title = "订单不存在"
	}
	
	return &Alert{
		Type:      fmt.Sprintf("trading_error_%s", errorCode),
		Level:     level,
		Title:     title,
		Message:   message,
		Timestamp: time.Now(),
		Data:      data,
	}
}

// CreateSystemAlert 创建系统告警
func CreateSystemAlert(alertType, level, title, message string, data map[string]interface{}) *Alert {
	return &Alert{
		Type:      alertType,
		Level:     level,
		Title:     title,
		Message:   message,
		Timestamp: time.Now(),
		Data:      data,
	}
}

// GetAlertHistory 获取告警历史（简单实现，实际可能需要持久化）
func (an *AlertNotifier) GetAlertHistory() map[string]time.Time {
	an.mu.RLock()
	defer an.mu.RUnlock()
	
	result := make(map[string]time.Time)
	for alertType, lastTime := range an.lastAlerts {
		result[alertType] = lastTime
	}
	
	return result
}

// ClearAlertHistory 清除告警历史
func (an *AlertNotifier) ClearAlertHistory() {
	an.mu.Lock()
	defer an.mu.Unlock()
	
	an.lastAlerts = make(map[string]time.Time)
}
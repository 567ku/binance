package monitoring

import (
	"sort"
	"time"
)

// RecordLatency 记录延迟
func (lm *LatencyMonitor) RecordLatency(latency time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	// 检查是否需要重置窗口
	if time.Since(lm.windowStart) > lm.windowDuration {
		lm.resetWindow()
	}
	
	lm.latencies = append(lm.latencies, latency)
	
	// 更新最大延迟
	if latency > lm.maxLatency {
		lm.maxLatency = latency
	}
	
	// 计算平均延迟
	lm.calculateStats()
}

// GetAverageLatency 获取平均延迟
func (lm *LatencyMonitor) GetAverageLatency() time.Duration {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	return lm.avgLatency
}

// GetMaxLatency 获取最大延迟
func (lm *LatencyMonitor) GetMaxLatency() time.Duration {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	return lm.maxLatency
}

// GetP95Latency 获取P95延迟
func (lm *LatencyMonitor) GetP95Latency() time.Duration {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	return lm.p95Latency
}

// GetP99Latency 获取P99延迟
func (lm *LatencyMonitor) GetP99Latency() time.Duration {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	return lm.p99Latency
}

// GetStats 获取统计信息
func (lm *LatencyMonitor) GetStats() map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	stats := map[string]interface{}{
		"sample_count":    len(lm.latencies),
		"avg_latency_ms":  lm.avgLatency.Milliseconds(),
		"max_latency_ms":  lm.maxLatency.Milliseconds(),
		"p95_latency_ms":  lm.p95Latency.Milliseconds(),
		"p99_latency_ms":  lm.p99Latency.Milliseconds(),
		"window_start":    lm.windowStart,
		"window_duration": lm.windowDuration,
	}
	
	return stats
}

// calculateStats 计算统计信息
func (lm *LatencyMonitor) calculateStats() {
	if len(lm.latencies) == 0 {
		return
	}
	
	// 计算平均延迟
	var total time.Duration
	for _, latency := range lm.latencies {
		total += latency
	}
	lm.avgLatency = total / time.Duration(len(lm.latencies))
	
	// 计算百分位延迟
	lm.calculatePercentiles()
}

// calculatePercentiles 计算百分位延迟
func (lm *LatencyMonitor) calculatePercentiles() {
	if len(lm.latencies) == 0 {
		return
	}
	
	// 复制并排序延迟数据
	sortedLatencies := make([]time.Duration, len(lm.latencies))
	copy(sortedLatencies, lm.latencies)
	sort.Slice(sortedLatencies, func(i, j int) bool {
		return sortedLatencies[i] < sortedLatencies[j]
	})
	
	// 计算P95
	p95Index := int(float64(len(sortedLatencies)) * 0.95)
	if p95Index >= len(sortedLatencies) {
		p95Index = len(sortedLatencies) - 1
	}
	lm.p95Latency = sortedLatencies[p95Index]
	
	// 计算P99
	p99Index := int(float64(len(sortedLatencies)) * 0.99)
	if p99Index >= len(sortedLatencies) {
		p99Index = len(sortedLatencies) - 1
	}
	lm.p99Latency = sortedLatencies[p99Index]
}

// resetWindow 重置统计窗口
func (lm *LatencyMonitor) resetWindow() {
	lm.latencies = lm.latencies[:0] // 清空但保留容量
	lm.maxLatency = 0
	lm.avgLatency = 0
	lm.p95Latency = 0
	lm.p99Latency = 0
	lm.windowStart = time.Now()
}

// IsLatencyHigh 检查延迟是否过高
func (lm *LatencyMonitor) IsLatencyHigh(threshold time.Duration) bool {
	return lm.GetAverageLatency() > threshold || lm.GetP95Latency() > threshold*2
}

// GetLatencyDistribution 获取延迟分布
func (lm *LatencyMonitor) GetLatencyDistribution() map[string]int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	distribution := map[string]int{
		"<100ms":    0,
		"100-500ms": 0,
		"500ms-1s":  0,
		"1s-5s":     0,
		">5s":       0,
	}
	
	for _, latency := range lm.latencies {
		ms := latency.Milliseconds()
		switch {
		case ms < 100:
			distribution["<100ms"]++
		case ms < 500:
			distribution["100-500ms"]++
		case ms < 1000:
			distribution["500ms-1s"]++
		case ms < 5000:
			distribution["1s-5s"]++
		default:
			distribution[">5s"]++
		}
	}
	
	return distribution
}

// Reset 重置所有统计
func (lm *LatencyMonitor) Reset() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	lm.resetWindow()
}
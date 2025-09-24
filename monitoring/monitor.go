package monitoring

import (
	"fmt"
	"log"
	"sync"
	"time"

	"binance-grid-trader/types"
)

// Monitor 监控器
type Monitor struct {
	mu                sync.RWMutex
	
	// 错误率监控
	errorRateMonitor  *ErrorRateMonitor
	
	// 延迟监控
	latencyMonitor    *LatencyMonitor
	
	// 余额监控
	balanceMonitor    *BalanceMonitor
	
	// 告警配置
	alertConfig       *AlertConfig
	
	// 告警通知器
	alertNotifier     *AlertNotifier
	
	// 监控状态
	isRunning         bool
	stopCh            chan struct{}
}

// ErrorRateMonitor 错误率监控器
type ErrorRateMonitor struct {
	mu              sync.RWMutex
	totalRequests   int64
	successRequests int64
	errorRequests   int64
	errorsByType    map[types.ErrorCode]int64
	windowStart     time.Time
	windowDuration  time.Duration
}

// LatencyMonitor 延迟监控器
type LatencyMonitor struct {
	mu              sync.RWMutex
	latencies       []time.Duration
	maxLatency      time.Duration
	avgLatency      time.Duration
	p95Latency      time.Duration
	p99Latency      time.Duration
	windowStart     time.Time
	windowDuration  time.Duration
}

// BalanceMonitor 余额监控器
type BalanceMonitor struct {
	mu              sync.RWMutex
	balances        map[string]float64  // asset -> balance
	balanceHistory  map[string][]BalanceRecord
	lowBalanceThresholds map[string]float64  // asset -> threshold
	lastUpdate      time.Time
	
	// 防抖机制相关字段
	pendingUpdates       map[string]*PendingUpdate    // asset -> pending update
	debounceConfigs      map[string]*DebounceConfig   // asset -> debounce config
	defaultDebounceConfig *DebounceConfig             // 默认防抖配置
}

// BalanceRecord 余额记录
type BalanceRecord struct {
	Timestamp time.Time
	Balance   float64
	Change    float64
}

// AlertConfig 告警配置
type AlertConfig struct {
	// 错误率告警阈值
	ErrorRateThreshold    float64       // 错误率阈值 (0.0-1.0)
	ErrorRateWindow       time.Duration // 错误率统计窗口
	
	// 延迟告警阈值
	LatencyThreshold      time.Duration // 延迟阈值
	LatencyWindow         time.Duration // 延迟统计窗口
	
	// 余额告警阈值
	LowBalanceThresholds  map[string]float64 // asset -> threshold
	BalanceChangeThreshold float64            // 余额变化阈值
	
	// 告警间隔
	AlertCooldown         time.Duration // 告警冷却时间
}

// AlertNotifier 告警通知器
type AlertNotifier struct {
	mu              sync.RWMutex
	lastAlerts      map[string]time.Time // alert type -> last alert time
	onAlert         func(alert *Alert)   // 告警回调
	alertConfig     *AlertConfig         // 告警配置引用
}

// Alert 告警信息
type Alert struct {
	Type        string                 `json:"type"`
	Level       string                 `json:"level"`     // INFO, WARN, ERROR, CRITICAL
	Title       string                 `json:"title"`
	Message     string                 `json:"message"`
	Timestamp   time.Time              `json:"timestamp"`
	Data        map[string]interface{} `json:"data"`
}

// NewMonitor 创建监控器
func NewMonitor(config *AlertConfig) *Monitor {
	return &Monitor{
		errorRateMonitor: NewErrorRateMonitor(config.ErrorRateWindow),
		latencyMonitor:   NewLatencyMonitor(config.LatencyWindow),
		balanceMonitor:   NewBalanceMonitor(config.LowBalanceThresholds),
		alertConfig:      config,
		alertNotifier:    NewAlertNotifier(config),
		stopCh:           make(chan struct{}),
	}
}

// NewErrorRateMonitor 创建错误率监控器
func NewErrorRateMonitor(windowDuration time.Duration) *ErrorRateMonitor {
	return &ErrorRateMonitor{
		errorsByType:   make(map[types.ErrorCode]int64),
		windowStart:    time.Now(),
		windowDuration: windowDuration,
	}
}

// NewLatencyMonitor 创建延迟监控器
func NewLatencyMonitor(windowDuration time.Duration) *LatencyMonitor {
	return &LatencyMonitor{
		latencies:      make([]time.Duration, 0, 1000),
		windowStart:    time.Now(),
		windowDuration: windowDuration,
	}
}

// NewBalanceMonitor 创建余额监控器
func NewBalanceMonitor(thresholds map[string]float64) *BalanceMonitor {
	// 默认防抖配置
	defaultConfig := &DebounceConfig{
		PercentageThreshold: 0.001,              // 0.1%变化阈值
		AbsoluteThreshold:   0.000001,           // 绝对值阈值
		TimeWindow:          500 * time.Millisecond, // 500ms时间窗口
		MaxDelay:            5 * time.Second,    // 最大延迟5秒
	}
	
	return &BalanceMonitor{
		balances:             make(map[string]float64),
		balanceHistory:       make(map[string][]BalanceRecord),
		lowBalanceThresholds: thresholds,
		lastUpdate:           time.Now(),
		pendingUpdates:       make(map[string]*PendingUpdate),
		debounceConfigs:      make(map[string]*DebounceConfig),
		defaultDebounceConfig: defaultConfig,
	}
}

// NewAlertNotifier 创建告警通知器
func NewAlertNotifier(config *AlertConfig) *AlertNotifier {
	return &AlertNotifier{
		lastAlerts:  make(map[string]time.Time),
		alertConfig: config,
	}
}

// Start 启动监控
func (m *Monitor) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.isRunning {
		return fmt.Errorf("monitor is already running")
	}
	
	m.isRunning = true
	
	// 启动监控协程
	go m.monitorLoop()
	
	log.Println("Monitor started")
	return nil
}

// Stop 停止监控
func (m *Monitor) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !m.isRunning {
		return nil
	}
	
	close(m.stopCh)
	m.isRunning = false
	
	log.Println("Monitor stopped")
	return nil
}

// monitorLoop 监控循环
func (m *Monitor) monitorLoop() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	pendingTicker := time.NewTicker(1 * time.Second) // 每1秒处理待处理更新
	defer ticker.Stop()
	defer pendingTicker.Stop()
	
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.performChecks()
		case <-pendingTicker.C:
			// 处理待处理的余额更新
			m.balanceMonitor.ProcessPendingUpdates()
		}
	}
}

// performChecks 执行监控检查
func (m *Monitor) performChecks() {
	// 检查错误率
	m.checkErrorRate()
	
	// 检查延迟
	m.checkLatency()
	
	// 检查余额
	m.checkBalance()
}

// RecordRequest 记录请求
func (m *Monitor) RecordRequest(success bool, latency time.Duration, errorCode types.ErrorCode) {
	// 记录错误率
	m.errorRateMonitor.RecordRequest(success, errorCode)
	
	// 记录延迟
	if success {
		m.latencyMonitor.RecordLatency(latency)
	}
}

// UpdateBalance 更新余额
func (m *Monitor) UpdateBalance(asset string, balance float64) {
	m.balanceMonitor.UpdateBalance(asset, balance)
}

// SetAlertCallback 设置告警回调
func (m *Monitor) SetAlertCallback(callback func(*Alert)) {
	m.alertNotifier.SetCallback(callback)
}

// GetStats 获取监控统计信息
func (m *Monitor) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	// 错误率统计
	errorStats := m.errorRateMonitor.GetStats()
	stats["error_rate"] = errorStats
	
	// 延迟统计
	latencyStats := m.latencyMonitor.GetStats()
	stats["latency"] = latencyStats
	
	// 余额统计
	balanceStats := m.balanceMonitor.GetStats()
	stats["balance"] = balanceStats
	
	return stats
}

// GetBalanceMonitor 获取余额监控器
func (m *Monitor) GetBalanceMonitor() *BalanceMonitor {
	return m.balanceMonitor
}

// RecordEvent 记录事件
func (m *Monitor) RecordEvent(eventType string, symbol string) {
	// 记录事件到监控系统
	// 这里可以根据需要扩展，比如记录事件计数、频率等
	log.Printf("Monitor: Recording event %s for symbol %s", eventType, symbol)
	
	// 可以在这里添加更多的监控逻辑，比如：
	// - 记录事件频率
	// - 检查异常事件模式
	// - 触发相关告警
}
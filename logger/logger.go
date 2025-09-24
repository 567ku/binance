package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
)

// Logger 日志记录器
type Logger struct {
	*logrus.Logger
	logFile *os.File
}

// NewLogger 创建新的日志记录器
func NewLogger(logDir string, level string) (*Logger, error) {
	// 确保日志目录存在
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// 创建日志文件
	logPath := filepath.Join(logDir, "app.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// 创建logrus实例
	log := logrus.New()

	// 设置日志级别
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	log.SetLevel(logLevel)

	// 设置输出格式
	log.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})

	// 设置输出到文件和控制台
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(multiWriter)

	return &Logger{
		Logger:  log,
		logFile: logFile,
	}, nil
}

// Close 关闭日志记录器
func (l *Logger) Close() error {
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

// WithComponent 添加组件字段
func (l *Logger) WithComponent(component string) *logrus.Entry {
	return l.WithField("component", component)
}

// WithOrderID 添加订单ID字段
func (l *Logger) WithOrderID(orderID string) *logrus.Entry {
	return l.WithField("order_id", orderID)
}

// WithSymbol 添加交易对字段
func (l *Logger) WithSymbol(symbol string) *logrus.Entry {
	return l.WithField("symbol", symbol)
}

// WithPrice 添加价格字段
func (l *Logger) WithPrice(price float64) *logrus.Entry {
	return l.WithField("price", price)
}

// WithQuantity 添加数量字段
func (l *Logger) WithQuantity(quantity float64) *logrus.Entry {
	return l.WithField("quantity", quantity)
}

// WithError 添加错误字段
func (l *Logger) WithError(err error) *logrus.Entry {
	return l.WithField("error", err.Error())
}

// LogOrderPlaced 记录订单下单
func (l *Logger) LogOrderPlaced(orderID, symbol, side string, price, quantity float64) {
	l.WithFields(logrus.Fields{
		"component": "grid_engine",
		"action":    "order_placed",
		"order_id":  orderID,
		"symbol":    symbol,
		"side":      side,
		"price":     price,
		"quantity":  quantity,
	}).Info("Order placed")
}

// LogOrderFilled 记录订单成交
func (l *Logger) LogOrderFilled(orderID, symbol string, price, quantity float64) {
	l.WithFields(logrus.Fields{
		"component": "grid_engine",
		"action":    "order_filled",
		"order_id":  orderID,
		"symbol":    symbol,
		"price":     price,
		"quantity":  quantity,
	}).Info("Order filled")
}

// LogOrderCanceled 记录订单取消
func (l *Logger) LogOrderCanceled(orderID, symbol, reason string) {
	l.WithFields(logrus.Fields{
		"component": "grid_engine",
		"action":    "order_canceled",
		"order_id":  orderID,
		"symbol":    symbol,
		"reason":    reason,
	}).Info("Order canceled")
}

// LogOrderRejected 记录订单拒绝
func (l *Logger) LogOrderRejected(orderID, symbol, reason string) {
	l.WithFields(logrus.Fields{
		"component": "grid_engine",
		"action":    "order_rejected",
		"order_id":  orderID,
		"symbol":    symbol,
		"reason":    reason,
	}).Warn("Order rejected")
}

// LogPriceUpdate 记录价格更新
func (l *Logger) LogPriceUpdate(symbol string, price float64) {
	l.WithFields(logrus.Fields{
		"component": "market_data",
		"action":    "price_update",
		"symbol":    symbol,
		"price":     price,
	}).Debug("Price updated")
}

// LogWebSocketConnected 记录WebSocket连接
func (l *Logger) LogWebSocketConnected(endpoint string) {
	l.WithFields(logrus.Fields{
		"component": "websocket",
		"action":    "connected",
		"endpoint":  endpoint,
	}).Info("WebSocket connected")
}

// LogWebSocketDisconnected 记录WebSocket断开
func (l *Logger) LogWebSocketDisconnected(endpoint string, reason string) {
	l.WithFields(logrus.Fields{
		"component": "websocket",
		"action":    "disconnected",
		"endpoint":  endpoint,
		"reason":    reason,
	}).Warn("WebSocket disconnected")
}

// LogWebSocketReconnecting 记录WebSocket重连
func (l *Logger) LogWebSocketReconnecting(endpoint string, attempt int) {
	l.WithFields(logrus.Fields{
		"component": "websocket",
		"action":    "reconnecting",
		"endpoint":  endpoint,
		"attempt":   attempt,
	}).Info("WebSocket reconnecting")
}

// LogDegradedModeEntered 记录进入降级模式
func (l *Logger) LogDegradedModeEntered(reason string) {
	l.WithFields(logrus.Fields{
		"component": "recovery",
		"action":    "degraded_mode_entered",
		"reason":    reason,
	}).Warn("Entered degraded mode")
}

// LogDegradedModeExited 记录退出降级模式
func (l *Logger) LogDegradedModeExited() {
	l.WithFields(logrus.Fields{
		"component": "recovery",
		"action":    "degraded_mode_exited",
	}).Info("Exited degraded mode")
}

// LogSnapshotCreated 记录快照创建
func (l *Logger) LogSnapshotCreated(timestamp time.Time) {
	l.WithFields(logrus.Fields{
		"component": "recovery",
		"action":    "snapshot_created",
		"timestamp": timestamp,
	}).Info("Snapshot created")
}

// LogSystemStartup 记录系统启动
func (l *Logger) LogSystemStartup() {
	l.WithFields(logrus.Fields{
		"component": "system",
		"action":    "startup",
	}).Info("System starting up")
}

// LogSystemShutdown 记录系统关闭
func (l *Logger) LogSystemShutdown() {
	l.WithFields(logrus.Fields{
		"component": "system",
		"action":    "shutdown",
	}).Info("System shutting down")
}

// LogGridInitialized 记录网格初始化
func (l *Logger) LogGridInitialized(symbol string, minPrice, maxPrice, stepSize float64, maxOrders int) {
	l.WithFields(logrus.Fields{
		"component":   "grid_engine",
		"action":      "grid_initialized",
		"symbol":      symbol,
		"min_price":   minPrice,
		"max_price":   maxPrice,
		"step_size":   stepSize,
		"max_orders":  maxOrders,
	}).Info("Grid initialized")
}

// LogGridStarted 记录网格启动
func (l *Logger) LogGridStarted(symbol string, currentPrice float64) {
	l.WithFields(logrus.Fields{
		"component":     "grid_engine",
		"action":        "grid_started",
		"symbol":        symbol,
		"current_price": currentPrice,
	}).Info("Grid trading started")
}

// LogGridStopped 记录网格停止
func (l *Logger) LogGridStopped(symbol string) {
	l.WithFields(logrus.Fields{
		"component": "grid_engine",
		"action":    "grid_stopped",
		"symbol":    symbol,
	}).Info("Grid trading stopped")
}

// LogAPIError 记录API错误
func (l *Logger) LogAPIError(endpoint string, statusCode int, message string) {
	l.WithFields(logrus.Fields{
		"component":   "api_client",
		"action":      "api_error",
		"endpoint":    endpoint,
		"status_code": statusCode,
		"message":     message,
	}).Error("API error occurred")
}

// LogRateLimitHit 记录触发限流
func (l *Logger) LogRateLimitHit(endpoint string, retryAfter int) {
	l.WithFields(logrus.Fields{
		"component":   "api_client",
		"action":      "rate_limit_hit",
		"endpoint":    endpoint,
		"retry_after": retryAfter,
	}).Warn("Rate limit hit")
}

// LogBalanceUpdate 记录余额更新
func (l *Logger) LogBalanceUpdate(asset string, balance, change float64) {
	l.WithFields(logrus.Fields{
		"component": "account",
		"action":    "balance_update",
		"asset":     asset,
		"balance":   balance,
		"change":    change,
	}).Info("Balance updated")
}

// LogPositionUpdate 记录持仓更新
func (l *Logger) LogPositionUpdate(symbol string, amount, entryPrice, unrealizedPnL float64) {
	l.WithFields(logrus.Fields{
		"component":      "account",
		"action":         "position_update",
		"symbol":         symbol,
		"amount":         amount,
		"entry_price":    entryPrice,
		"unrealized_pnl": unrealizedPnL,
	}).Info("Position updated")
}

// LogRecoveryStarted 记录恢复开始
func (l *Logger) LogRecoveryStarted() {
	l.WithFields(logrus.Fields{
		"component": "recovery",
		"action":    "recovery_started",
	}).Info("System recovery started")
}

// LogRecoveryCompleted 记录恢复完成
func (l *Logger) LogRecoveryCompleted(duration time.Duration) {
	l.WithFields(logrus.Fields{
		"component": "recovery",
		"action":    "recovery_completed",
		"duration":  duration.String(),
	}).Info("System recovery completed")
}

// LogEventReplayed 记录事件重放
func (l *Logger) LogEventReplayed(eventType string, count int) {
	l.WithFields(logrus.Fields{
		"component":  "recovery",
		"action":     "events_replayed",
		"event_type": eventType,
		"count":      count,
	}).Info("Events replayed")
}

// LogConfigLoaded 记录配置加载
func (l *Logger) LogConfigLoaded(configPath string) {
	l.WithFields(logrus.Fields{
		"component":   "config",
		"action":      "config_loaded",
		"config_path": configPath,
	}).Info("Configuration loaded")
}

// LogStatistics 记录统计信息
func (l *Logger) LogStatistics(stats map[string]interface{}) {
	l.WithFields(logrus.Fields{
		"component": "statistics",
		"action":    "stats_update",
		"stats":     stats,
	}).Info("Statistics updated")
}
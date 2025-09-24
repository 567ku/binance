package errors

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"binance-grid-trader/storage"
	"binance-grid-trader/types"
)

// ErrorHandler 错误处理器
type ErrorHandler struct {
	mu                sync.RWMutex
	storage           *storage.Storage
	
	// 速率限制跟踪
	rateLimitTracker  *RateLimitTracker
	
	// 错误统计
	errorStats        map[types.ErrorCode]int
	lastErrorTime     map[types.ErrorCode]time.Time
	
	// 回调函数
	onError           func(*types.TradingError)
	onRateLimit       func(time.Duration)
	onInsufficientBalance func(string, float64, float64) // asset, required, available
}

// TokenBucket Token桶限流器
type TokenBucket struct {
	mu           sync.Mutex
	capacity     int           // 桶容量
	tokens       int           // 当前令牌数
	refillRate   int           // 每秒补充令牌数
	lastRefill   time.Time     // 上次补充时间
	windowSize   time.Duration // 时间窗口大小
}

// BackoffManager 退避管理器
type BackoffManager struct {
	mu              sync.RWMutex
	backoffState    map[string]*BackoffState // endpoint -> backoff state
	baseDelay       time.Duration            // 基础延迟
	maxDelay        time.Duration            // 最大延迟
	multiplier      float64                  // 退避倍数
}

// BackoffState 退避状态
type BackoffState struct {
	attempts    int           // 连续失败次数
	nextRetry   time.Time     // 下次重试时间
	currentDelay time.Duration // 当前延迟时间
}

// RateLimitTracker 速率限制跟踪器
type RateLimitTracker struct {
	mu              sync.RWMutex
	tokenBucket     *TokenBucket      // Token桶（10秒窗口）
	backoffManager  *BackoffManager   // 退避管理器
	
	// 多时间窗口支持
	requestCounts   map[string]map[time.Duration]int    // endpoint -> window -> count
	windowStart     map[string]map[time.Duration]time.Time // endpoint -> window -> start time
	limits          map[string]map[time.Duration]int    // endpoint -> window -> limit
	
	// 权重跟踪
	weightCounts    map[time.Duration]int               // window -> weight count
	weightStart     map[time.Duration]time.Time        // window -> start time
	weightLimits    map[time.Duration]int               // window -> weight limit
}

// NewErrorHandler 创建错误处理器
func NewErrorHandler(storage *storage.Storage) *ErrorHandler {
	return &ErrorHandler{
		storage:          storage,
		rateLimitTracker: NewRateLimitTracker(),
		errorStats:       make(map[types.ErrorCode]int),
		lastErrorTime:    make(map[types.ErrorCode]time.Time),
	}
}

// NewTokenBucket 创建Token桶
func NewTokenBucket(capacity int, refillRate int, windowSize time.Duration) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity, // 初始满桶
		refillRate: refillRate,
		lastRefill: time.Now(),
		windowSize: windowSize,
	}
}

// NewBackoffManager 创建退避管理器
func NewBackoffManager() *BackoffManager {
	return &BackoffManager{
		backoffState: make(map[string]*BackoffState),
		baseDelay:    1 * time.Second,   // 基础延迟1秒
		maxDelay:     300 * time.Second, // 最大延迟5分钟
		multiplier:   2.0,               // 指数退避倍数
	}
}

// NewRateLimitTracker 创建速率限制跟踪器
func NewRateLimitTracker() *RateLimitTracker {
	tracker := &RateLimitTracker{
		// 币安API限制：10秒窗口内最多300单，1分钟内最多1200单，权重2400/1分钟
		tokenBucket:    NewTokenBucket(300, 300, 10*time.Second), // 300个令牌，10秒窗口，每10秒补充300个
		backoffManager: NewBackoffManager(),
		requestCounts:  make(map[string]map[time.Duration]int),
		windowStart:    make(map[string]map[time.Duration]time.Time),
		limits:         make(map[string]map[time.Duration]int),
		weightCounts:   make(map[time.Duration]int),
		weightStart:    make(map[time.Duration]time.Time),
		weightLimits:   make(map[time.Duration]int),
	}
	
	// 设置默认限制（对齐新口径）
	tracker.SetLimit("order", 10*time.Second, 300)    // 下单接口 300次/10秒
	tracker.SetLimit("order", 1*time.Minute, 1200)    // 下单接口 1200次/1分钟
	tracker.SetLimit("cancel", 10*time.Second, 300)   // 撤单接口 300次/10秒
	tracker.SetLimit("cancel", 1*time.Minute, 1200)   // 撤单接口 1200次/1分钟
	tracker.SetLimit("query", 10*time.Second, 300)    // 查询接口 300次/10秒
	tracker.SetLimit("query", 1*time.Minute, 1200)    // 查询接口 1200次/1分钟
	tracker.SetLimit("account", 10*time.Second, 300)  // 账户接口 300次/10秒
	tracker.SetLimit("account", 1*time.Minute, 1200)  // 账户接口 1200次/1分钟
	
	// 设置权重限制
	tracker.SetWeightLimit(1*time.Minute, 2400)       // 权重限制 2400/1分钟
	
	return tracker
}

// SetCallbacks 设置回调函数
func (eh *ErrorHandler) SetCallbacks(
	onError func(*types.TradingError),
	onRateLimit func(time.Duration),
	onInsufficientBalance func(string, float64, float64),
) {
	eh.mu.Lock()
	defer eh.mu.Unlock()
	
	eh.onError = onError
	eh.onRateLimit = onRateLimit
	eh.onInsufficientBalance = onInsufficientBalance
}

// HandleError 处理错误
func (eh *ErrorHandler) HandleError(err error, context map[string]interface{}) *types.TradingError {
	tradingError := eh.classifyError(err, context)
	
	eh.mu.Lock()
	eh.errorStats[tradingError.Code]++
	eh.lastErrorTime[tradingError.Code] = time.Now()
	eh.mu.Unlock()
	
	// 记录错误到存储
	if err := eh.storage.WriteRejection(&types.Rejection{
		Timestamp:   time.Now().Unix(),
		Code:        0, // 暂时设为0，需要根据ErrorCode映射
		Message:     tradingError.Message,
		RawResponse: context,
	}); err != nil {
		log.Printf("Failed to write rejection: %v", err)
	}
	
	// 根据错误类型执行特定处理
	eh.handleSpecificError(tradingError, context)
	
	// 触发回调
	if eh.onError != nil {
		eh.onError(tradingError)
	}
	
	log.Printf("Handled error: %s - %s (Retryable: %v)", 
		tradingError.Code, tradingError.Message, tradingError.Retryable)
	
	return tradingError
}

// classifyError 分类错误
func (eh *ErrorHandler) classifyError(err error, context map[string]interface{}) *types.TradingError {
	errorMsg := err.Error()
	
	// 速率限制错误
	if eh.isRateLimitError(errorMsg) {
		return &types.TradingError{
			Code:       types.ErrorCodeRateLimit,
			Message:    errorMsg,
			Retryable:  true,
			RetryAfter: 60 * time.Second, // 1分钟后重试
			Context:    context,
		}
	}
	
	// 余额不足错误
	if eh.isInsufficientBalanceError(errorMsg) {
		return &types.TradingError{
			Code:       types.ErrorCodeInsufficientBalance,
			Message:    errorMsg,
			Retryable:  false, // 余额不足通常不可重试
			RetryAfter: 0,
			Context:    context,
		}
	}
	
	// 价格超出区间错误
	if eh.isPriceOutOfRangeError(errorMsg) {
		return &types.TradingError{
			Code:       types.ErrorCodePriceOutOfRange,
			Message:    errorMsg,
			Retryable:  false, // 价格问题需要重新计算
			RetryAfter: 0,
			Context:    context,
		}
	}
	
	// 网络错误
	if eh.isNetworkError(errorMsg) {
		return &types.TradingError{
			Code:       types.ErrorCodeNetworkError,
			Message:    errorMsg,
			Retryable:  true,
			RetryAfter: 5 * time.Second, // 5秒后重试
			Context:    context,
		}
	}
	
	// 订单不存在错误
	if eh.isOrderNotFoundError(errorMsg) {
		return &types.TradingError{
			Code:       types.ErrorCodeOrderNotFound,
			Message:    errorMsg,
			Retryable:  false,
			RetryAfter: 0,
			Context:    context,
		}
	}
	
	// 市场数据错误
	if eh.isMarketDataError(errorMsg) {
		return &types.TradingError{
			Code:       types.ErrorCodeMarketDataError,
			Message:    errorMsg,
			Retryable:  true,
			RetryAfter: 10 * time.Second, // 10秒后重试
			Context:    context,
		}
	}
	
	// 权限错误
	if eh.isPermissionError(errorMsg) {
		return &types.TradingError{
			Code:       types.ErrorCodePermissionDenied,
			Message:    errorMsg,
			Retryable:  false,
			RetryAfter: 0,
			Context:    context,
		}
	}
	
	// 默认为未知错误
	return &types.TradingError{
		Code:       types.ErrorCodeUnknown,
		Message:    errorMsg,
		Retryable:  false,
		RetryAfter: 0,
		Context:    context,
	}
}

// handleSpecificError 处理特定类型的错误
func (eh *ErrorHandler) handleSpecificError(tradingError *types.TradingError, context map[string]interface{}) {
	switch tradingError.Code {
	case types.ErrorCodeRateLimit:
		eh.handleRateLimitError(tradingError, context)
		
	case types.ErrorCodeInsufficientBalance:
		eh.handleInsufficientBalanceError(tradingError, context)
		
	case types.ErrorCodePriceOutOfRange:
		eh.handlePriceOutOfRangeError(tradingError, context)
		
	case types.ErrorCodeNetworkError:
		eh.handleNetworkError(tradingError, context)
		
	default:
		// 其他错误的通用处理
		log.Printf("Unhandled error type: %s", tradingError.Code)
	}
}

// handleRateLimitError 处理速率限制错误
func (eh *ErrorHandler) handleRateLimitError(tradingError *types.TradingError, context map[string]interface{}) {
	log.Printf("Rate limit hit, waiting %v before retry", tradingError.RetryAfter)
	
	// 触发速率限制回调
	if eh.onRateLimit != nil {
		eh.onRateLimit(tradingError.RetryAfter)
	}
	
	// 更新速率限制跟踪器
	if endpoint, ok := context["endpoint"].(string); ok {
		eh.rateLimitTracker.RecordRateLimit(endpoint)
	}
}

// handleInsufficientBalanceError 处理余额不足错误
func (eh *ErrorHandler) handleInsufficientBalanceError(tradingError *types.TradingError, context map[string]interface{}) {
	asset := "USDT" // 默认资产
	var required, available float64
	
	if a, ok := context["asset"].(string); ok {
		asset = a
	}
	if r, ok := context["required_balance"].(float64); ok {
		required = r
	}
	if a, ok := context["available_balance"].(float64); ok {
		available = a
	}
	
	log.Printf("Insufficient balance: %s, required: %.6f, available: %.6f", 
		asset, required, available)
	
	// 触发余额不足回调
	if eh.onInsufficientBalance != nil {
		eh.onInsufficientBalance(asset, required, available)
	}
}

// handlePriceOutOfRangeError 处理价格超出区间错误
func (eh *ErrorHandler) handlePriceOutOfRangeError(tradingError *types.TradingError, context map[string]interface{}) {
	log.Printf("Price out of range error: %s", tradingError.Message)
	
	// 可以在这里触发价格重新计算逻辑
	if symbol, ok := context["symbol"].(string); ok {
		log.Printf("Symbol %s price needs recalculation", symbol)
	}
}

// handleNetworkError 处理网络错误
func (eh *ErrorHandler) handleNetworkError(tradingError *types.TradingError, context map[string]interface{}) {
	log.Printf("Network error occurred, will retry in %v", tradingError.RetryAfter)
	
	// 网络错误可能需要检查连接状态
	if operation, ok := context["operation"].(string); ok {
		log.Printf("Network error during operation: %s", operation)
	}
}

// 错误识别函数
func (eh *ErrorHandler) isRateLimitError(errorMsg string) bool {
	rateLimitKeywords := []string{
		"rate limit",
		"too many requests",
		"request weight",
		"429",
		"rate_limit_exceeded",
	}
	
	for _, keyword := range rateLimitKeywords {
		if contains(errorMsg, keyword) {
			return true
		}
	}
	return false
}

func (eh *ErrorHandler) isInsufficientBalanceError(errorMsg string) bool {
	balanceKeywords := []string{
		"insufficient balance",
		"insufficient funds",
		"not enough balance",
		"balance not sufficient",
		"account has insufficient balance",
	}
	
	for _, keyword := range balanceKeywords {
		if contains(errorMsg, keyword) {
			return true
		}
	}
	return false
}

func (eh *ErrorHandler) isPriceOutOfRangeError(errorMsg string) bool {
	priceKeywords := []string{
		"price out of range",
		"invalid price",
		"price filter",
		"min price",
		"max price",
		"tick size",
	}
	
	for _, keyword := range priceKeywords {
		if contains(errorMsg, keyword) {
			return true
		}
	}
	return false
}

func (eh *ErrorHandler) isNetworkError(errorMsg string) bool {
	networkKeywords := []string{
		"connection refused",
		"timeout",
		"network error",
		"connection reset",
		"no route to host",
		"connection timed out",
	}
	
	for _, keyword := range networkKeywords {
		if contains(errorMsg, keyword) {
			return true
		}
	}
	return false
}

func (eh *ErrorHandler) isOrderNotFoundError(errorMsg string) bool {
	orderKeywords := []string{
		"order not found",
		"unknown order",
		"order does not exist",
		"invalid order id",
	}
	
	for _, keyword := range orderKeywords {
		if contains(errorMsg, keyword) {
			return true
		}
	}
	return false
}

func (eh *ErrorHandler) isMarketDataError(errorMsg string) bool {
	marketKeywords := []string{
		"market data",
		"symbol not found",
		"invalid symbol",
		"market closed",
	}
	
	for _, keyword := range marketKeywords {
		if contains(errorMsg, keyword) {
			return true
		}
	}
	return false
}

func (eh *ErrorHandler) isPermissionError(errorMsg string) bool {
	permissionKeywords := []string{
		"permission denied",
		"unauthorized",
		"forbidden",
		"access denied",
		"invalid api key",
		"signature verification failed",
	}
	
	for _, keyword := range permissionKeywords {
		if contains(errorMsg, keyword) {
			return true
		}
	}
	return false
}

// RateLimitTracker 方法

// SetLimit 设置端点限制
func (rlt *RateLimitTracker) SetLimit(endpoint string, window time.Duration, limit int) {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	
	if rlt.limits[endpoint] == nil {
		rlt.limits[endpoint] = make(map[time.Duration]int)
	}
	rlt.limits[endpoint][window] = limit
}

// SetWeightLimit 设置权重限制
func (rlt *RateLimitTracker) SetWeightLimit(window time.Duration, limit int) {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	
	rlt.weightLimits[window] = limit
}

// CanMakeRequest 检查是否可以发起请求
func (rlt *RateLimitTracker) CanMakeRequest(endpoint string) bool {
	return rlt.CanMakeRequestWithWeight(endpoint, 1)
}

// CanMakeRequestWithWeight 检查是否可以发起带权重的请求
func (rlt *RateLimitTracker) CanMakeRequestWithWeight(endpoint string, weight int) bool {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	
	now := time.Now()
	
	// 检查所有时间窗口的限制
	if rlt.limits[endpoint] != nil {
		for window, limit := range rlt.limits[endpoint] {
			// 初始化或重置窗口
			if rlt.requestCounts[endpoint] == nil {
				rlt.requestCounts[endpoint] = make(map[time.Duration]int)
				rlt.windowStart[endpoint] = make(map[time.Duration]time.Time)
			}
			
			windowStart, exists := rlt.windowStart[endpoint][window]
			if !exists || now.Sub(windowStart) >= window {
				// 重置窗口
				rlt.requestCounts[endpoint][window] = 0
				rlt.windowStart[endpoint][window] = now
			}
			
			// 检查是否超过限制
			if rlt.requestCounts[endpoint][window] >= limit {
				return false
			}
		}
	}
	
	// 检查权重限制
	for window, limit := range rlt.weightLimits {
		windowStart, exists := rlt.weightStart[window]
		if !exists || now.Sub(windowStart) >= window {
			// 重置权重窗口
			rlt.weightCounts[window] = 0
			rlt.weightStart[window] = now
		}
		
		// 检查权重是否超过限制
		if rlt.weightCounts[window]+weight > limit {
			return false
		}
	}
	
	return true
}

// RecordRequest 记录请求
func (rlt *RateLimitTracker) RecordRequest(endpoint string) {
	rlt.RecordRequestWithWeight(endpoint, 1)
}

// RecordRequestWithWeight 记录带权重的请求
func (rlt *RateLimitTracker) RecordRequestWithWeight(endpoint string, weight int) {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	
	// 记录请求计数
	if rlt.requestCounts[endpoint] != nil {
		for window := range rlt.limits[endpoint] {
			rlt.requestCounts[endpoint][window]++
		}
	}
	
	// 记录权重
	for window := range rlt.weightLimits {
		rlt.weightCounts[window] += weight
	}
}

// RecordRateLimit 记录速率限制
func (rlt *RateLimitTracker) RecordRateLimit(endpoint string) {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	
	// 将计数设置为限制值，防止继续请求
	if rlt.limits[endpoint] != nil {
		for window, limit := range rlt.limits[endpoint] {
			if rlt.requestCounts[endpoint] == nil {
				rlt.requestCounts[endpoint] = make(map[time.Duration]int)
			}
			rlt.requestCounts[endpoint][window] = limit
		}
	}
}

// GetRemainingRequests 获取剩余请求数
func (rlt *RateLimitTracker) GetRemainingRequests(endpoint string) int {
	rlt.mu.RLock()
	defer rlt.mu.RUnlock()
	
	minRemaining := math.MaxInt32
	
	if rlt.limits[endpoint] != nil {
		for window, limit := range rlt.limits[endpoint] {
			if rlt.requestCounts[endpoint] != nil {
				count := rlt.requestCounts[endpoint][window]
				remaining := limit - count
				if remaining < minRemaining {
					minRemaining = remaining
				}
			} else {
				if limit < minRemaining {
					minRemaining = limit
				}
			}
		}
	}
	
	if minRemaining == math.MaxInt32 {
		return 0
	}
	return minRemaining
}

// GetRemainingWeight 获取剩余权重
func (rlt *RateLimitTracker) GetRemainingWeight(window time.Duration) int {
	rlt.mu.RLock()
	defer rlt.mu.RUnlock()
	
	limit, exists := rlt.weightLimits[window]
	if !exists {
		return 0
	}
	
	count := rlt.weightCounts[window]
	return limit - count
}

// ErrorHandler 统计方法

// GetErrorStats 获取错误统计
func (eh *ErrorHandler) GetErrorStats() map[types.ErrorCode]int {
	eh.mu.RLock()
	defer eh.mu.RUnlock()
	
	stats := make(map[types.ErrorCode]int)
	for code, count := range eh.errorStats {
		stats[code] = count
	}
	
	return stats
}

// GetLastErrorTime 获取最后错误时间
func (eh *ErrorHandler) GetLastErrorTime(code types.ErrorCode) time.Time {
	eh.mu.RLock()
	defer eh.mu.RUnlock()
	
	return eh.lastErrorTime[code]
}

// ResetErrorStats 重置错误统计
func (eh *ErrorHandler) ResetErrorStats() {
	eh.mu.Lock()
	defer eh.mu.Unlock()
	
	eh.errorStats = make(map[types.ErrorCode]int)
	eh.lastErrorTime = make(map[types.ErrorCode]time.Time)
}

// ShouldRetry 判断是否应该重试
func (eh *ErrorHandler) ShouldRetry(tradingError *types.TradingError, attemptCount int, maxAttempts int) bool {
	if !tradingError.Retryable {
		return false
	}
	
	if attemptCount >= maxAttempts {
		return false
	}
	
	// 检查是否在重试间隔内
	if tradingError.RetryAfter > 0 {
		lastErrorTime := eh.GetLastErrorTime(tradingError.Code)
		if time.Since(lastErrorTime) < tradingError.RetryAfter {
			return false
		}
	}
	
	return true
}

// CreateRetryableOperation 创建可重试操作
func (eh *ErrorHandler) CreateRetryableOperation(
	operation func() error,
	maxAttempts int,
	context map[string]interface{},
) error {
	var lastError *types.TradingError
	
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := operation()
		if err == nil {
			return nil // 成功
		}
		
		// 处理错误
		lastError = eh.HandleError(err, context)
		
		// 检查是否应该重试
		if !eh.ShouldRetry(lastError, attempt, maxAttempts) {
			break
		}
		
		// 等待重试间隔
		if lastError.RetryAfter > 0 {
			log.Printf("Waiting %v before retry (attempt %d/%d)", 
				lastError.RetryAfter, attempt, maxAttempts)
			time.Sleep(lastError.RetryAfter)
		}
		
		log.Printf("Retrying operation (attempt %d/%d)", attempt+1, maxAttempts)
	}
	
	return fmt.Errorf("operation failed after %d attempts: %s", maxAttempts, lastError.Message)
}

// 辅助函数
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || 
		(len(s) > len(substr) && 
			(s[:len(substr)] == substr || 
			 s[len(s)-len(substr):] == substr ||
			 containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Token桶方法

// TryConsume 尝试消费令牌
func (tb *TokenBucket) TryConsume(tokens int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	
	tb.refill()
	
	if tb.tokens >= tokens {
		tb.tokens -= tokens
		return true
	}
	
	return false
}

// refill 补充令牌
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)
	
	// 计算应该补充的令牌数
	tokensToAdd := int(elapsed.Seconds() * float64(tb.refillRate) / tb.windowSize.Seconds())
	
	if tokensToAdd > 0 {
		tb.tokens += tokensToAdd
		if tb.tokens > tb.capacity {
			tb.tokens = tb.capacity
		}
		tb.lastRefill = now
	}
}

// GetAvailableTokens 获取可用令牌数
func (tb *TokenBucket) GetAvailableTokens() int {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	
	tb.refill()
	return tb.tokens
}

// Reset 重置Token桶
func (tb *TokenBucket) Reset() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	
	tb.tokens = tb.capacity
	tb.lastRefill = time.Now()
}

// 退避管理器方法

// ShouldRetry 检查是否应该重试
func (bm *BackoffManager) ShouldRetry(endpoint string) bool {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	state, exists := bm.backoffState[endpoint]
	if !exists {
		return true // 首次请求，允许
	}
	
	return time.Now().After(state.nextRetry)
}

// RecordFailure 记录失败并计算下次重试时间
func (bm *BackoffManager) RecordFailure(endpoint string, errorCode string) time.Duration {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	state, exists := bm.backoffState[endpoint]
	if !exists {
		state = &BackoffState{
			attempts:     0,
			currentDelay: bm.baseDelay,
		}
		bm.backoffState[endpoint] = state
	}
	
	state.attempts++
	
	// 对于429和418错误，使用指数退避
	if errorCode == "429" || errorCode == "418" {
		// 指数退避：delay = baseDelay * (multiplier ^ attempts)
		delay := time.Duration(float64(bm.baseDelay) * math.Pow(bm.multiplier, float64(state.attempts-1)))
		
		// 限制最大延迟
		if delay > bm.maxDelay {
			delay = bm.maxDelay
		}
		
		state.currentDelay = delay
		state.nextRetry = time.Now().Add(delay)
		
		log.Printf("Rate limit error %s for endpoint %s, backing off for %v (attempt %d)", 
			errorCode, endpoint, delay, state.attempts)
		
		return delay
	}
	
	// 其他错误使用固定延迟
	state.currentDelay = bm.baseDelay
	state.nextRetry = time.Now().Add(bm.baseDelay)
	
	return bm.baseDelay
}

// RecordSuccess 记录成功，重置退避状态
func (bm *BackoffManager) RecordSuccess(endpoint string) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	
	delete(bm.backoffState, endpoint)
}

// GetNextRetryTime 获取下次重试时间
func (bm *BackoffManager) GetNextRetryTime(endpoint string) time.Time {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	state, exists := bm.backoffState[endpoint]
	if !exists {
		return time.Now() // 立即可重试
	}
	
	return state.nextRetry
}

// GetCurrentDelay 获取当前延迟时间
func (bm *BackoffManager) GetCurrentDelay(endpoint string) time.Duration {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	state, exists := bm.backoffState[endpoint]
	if !exists {
		return 0
	}
	
	return state.currentDelay
}

// GetAttempts 获取连续失败次数
func (bm *BackoffManager) GetAttempts(endpoint string) int {
	bm.mu.RLock()
	defer bm.mu.RUnlock()
	
	state, exists := bm.backoffState[endpoint]
	if !exists {
		return 0
	}
	
	return state.attempts
}

// 增强的RateLimitTracker方法

// CanMakeRequestWithTokens 检查Token桶检查是否可以发起请求
func (rlt *RateLimitTracker) CanMakeRequestWithTokens(endpoint string, tokens int) bool {
	// 使用新的权重检查方法
	return rlt.CanMakeRequestWithWeight(endpoint, tokens)
}

// RecordRequestWithBackoff 记录请求并处理退避
func (rlt *RateLimitTracker) RecordRequestWithBackoff(endpoint string, success bool, errorCode string) time.Duration {
	// 记录请求
	rlt.RecordRequest(endpoint)
	
	// 处理退避逻辑
	if success {
		rlt.backoffManager.RecordSuccess(endpoint)
		return 0
	} else {
		return rlt.backoffManager.RecordFailure(endpoint, errorCode)
	}
}

// IsInBackoff 检查是否处于退避状态
func (rlt *RateLimitTracker) IsInBackoff(endpoint string) bool {
	rlt.mu.RLock()
	defer rlt.mu.RUnlock()
	
	return !rlt.backoffManager.ShouldRetry(endpoint)
}

// GetBackoffInfo 获取退避信息
func (rlt *RateLimitTracker) GetBackoffInfo(endpoint string) (bool, time.Time, time.Duration, int) {
	rlt.mu.RLock()
	defer rlt.mu.RUnlock()
	
	inBackoff := !rlt.backoffManager.ShouldRetry(endpoint)
	nextRetry := rlt.backoffManager.GetNextRetryTime(endpoint)
	currentDelay := rlt.backoffManager.GetCurrentDelay(endpoint)
	attempts := rlt.backoffManager.GetAttempts(endpoint)
	
	return inBackoff, nextRetry, currentDelay, attempts
}

// GetTokenBucketStatus 获取Token桶状态
func (rlt *RateLimitTracker) GetTokenBucketStatus() (int, int) {
	rlt.mu.RLock()
	defer rlt.mu.RUnlock()
	
	available := rlt.tokenBucket.GetAvailableTokens()
	capacity := rlt.tokenBucket.capacity
	
	return available, capacity
}

// ResetTokenBucket 重置Token桶
func (rlt *RateLimitTracker) ResetTokenBucket() {
	rlt.mu.Lock()
	defer rlt.mu.Unlock()
	
	rlt.tokenBucket.Reset()
}

// WaitForTokens 等待足够的令牌
func (rlt *RateLimitTracker) WaitForTokens(tokens int) time.Duration {
	available := rlt.tokenBucket.GetAvailableTokens()
	if available >= tokens {
		return 0
	}
	
	// 计算需要等待的时间
	needed := tokens - available
	refillRate := float64(rlt.tokenBucket.refillRate) / rlt.tokenBucket.windowSize.Seconds()
	waitTime := time.Duration(float64(needed)/refillRate) * time.Second
	
	return waitTime
}

// 增强的错误处理方法

// HandleRateLimitWithBackoff 处理速率限制错误并应用退避
func (eh *ErrorHandler) HandleRateLimitWithBackoff(err error, endpoint string, context map[string]interface{}) *types.TradingError {
	tradingError := eh.classifyError(err, context)
	
	// 提取错误代码
	errorCode := "429" // 默认为429
	if code, ok := context["error_code"].(string); ok {
		errorCode = code
	}
	
	// 记录失败并获取退避时间
	backoffDuration := eh.rateLimitTracker.RecordRequestWithBackoff(endpoint, false, errorCode)
	
	// 更新重试时间
	tradingError.RetryAfter = backoffDuration
	
	// 记录错误统计
	eh.mu.Lock()
	eh.errorStats[tradingError.Code]++
	eh.lastErrorTime[tradingError.Code] = time.Now()
	eh.mu.Unlock()
	
	// 记录到存储
	if err := eh.storage.WriteRejection(&types.Rejection{
		Timestamp:   time.Now().Unix(),
		Code:        0, // 暂时设为0，需要根据ErrorCode映射
		Message:     tradingError.Message,
		RawResponse: context,
	}); err != nil {
		log.Printf("Failed to write rejection: %v", err)
	}
	
	log.Printf("Rate limit error %s for endpoint %s, backing off for %v", 
		errorCode, endpoint, backoffDuration)
	
	return tradingError
}

// CanMakeOrderRequest 检查是否可以发起下单请求
func (eh *ErrorHandler) CanMakeOrderRequest() bool {
	// 检查Token桶（下单需要1个令牌）
	return eh.rateLimitTracker.CanMakeRequestWithTokens("order", 1)
}

// RecordOrderRequest 记录下单请求结果
func (eh *ErrorHandler) RecordOrderRequest(success bool, errorCode string) time.Duration {
	return eh.rateLimitTracker.RecordRequestWithBackoff("order", success, errorCode)
}

// GetRateLimitStatus 获取限流状态
func (eh *ErrorHandler) GetRateLimitStatus() map[string]interface{} {
	available, capacity := eh.rateLimitTracker.GetTokenBucketStatus()
	
	status := map[string]interface{}{
		"token_bucket": map[string]interface{}{
			"available": available,
			"capacity":  capacity,
		},
		"backoff_states": make(map[string]interface{}),
	}
	
	// 获取各端点的退避状态
	endpoints := []string{"order", "cancel", "query", "account"}
	backoffStates := status["backoff_states"].(map[string]interface{})
	
	for _, endpoint := range endpoints {
		inBackoff, nextRetry, currentDelay, attempts := eh.rateLimitTracker.GetBackoffInfo(endpoint)
		backoffStates[endpoint] = map[string]interface{}{
			"in_backoff":     inBackoff,
			"next_retry":     nextRetry,
			"current_delay":  currentDelay,
			"attempts":       attempts,
		}
	}
	
	return status
}
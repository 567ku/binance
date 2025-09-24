package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"binance-grid-trader/binance"
	"binance-grid-trader/config"
	"binance-grid-trader/errors"
	"binance-grid-trader/monitoring"
	"binance-grid-trader/multigrid"
	"binance-grid-trader/pairing"
	"binance-grid-trader/recovery"
	"binance-grid-trader/reducer"
	"binance-grid-trader/storage"
	"binance-grid-trader/types"
)

// GridTrader 网格交易器主结构
type GridTrader struct {
	config           *config.Config
	storage          *storage.Storage
	binanceClient    *binance.Client
	wsClient         *binance.WSClient
	errorHandler     *errors.ErrorHandler
	pairingEngine    *pairing.PairingEngine
	recoveryManager  *recovery.RecoveryManager
	multiGridManager *multigrid.MultiGridManager
	degradedMode     *binance.DegradedMode
	reducer          *reducer.Reducer
	monitor          *monitoring.Monitor
	
	// 运行时状态
	ctx              context.Context
	cancel           context.CancelFunc
	isRunning        bool
	systemState      types.SystemState
}

func main() {
	log.Println("Starting Binance Grid Trader...")
	
	// 创建主交易器
	trader, err := NewGridTrader("config.json")
	if err != nil {
		log.Fatalf("Failed to create grid trader: %v", err)
	}
	
	// 启动交易器
	if err := trader.Start(); err != nil {
		log.Fatalf("Failed to start grid trader: %v", err)
	}
	
	// 等待中断信号
	trader.WaitForShutdown()
	
	log.Println("Binance Grid Trader stopped")
}

// NewGridTrader 创建网格交易器
func NewGridTrader(configPath string) (*GridTrader, error) {
	// 加载配置
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// 创建存储
	storage, err := storage.NewStorage(cfg.System.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// 加载API凭证
	credentials, err := config.LoadAPICredentials(cfg.API.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load API credentials: %w", err)
	}

	// 创建币安客户端
	binanceClient := binance.NewClient(cfg, credentials)

	// 创建WebSocket客户端
	wsClient := binance.NewWSClient(cfg.API.WsURL)

	// 创建错误处理器
	errorHandler := errors.NewErrorHandler(storage)

	// 创建配对引擎
	pairingEngine := pairing.NewPairingEngine(
		storage.GetPairTable(),
		storage.GetOrderTable(),
		storage.GetExecutionTable(),
	)

	// 创建Reducer
	reducer := reducer.NewReducer(storage)

	// 设置Reducer到binanceClient
	binanceClient.SetReducer(reducer)

	// 创建降级模式管理器 - 2秒轮询间隔
	degradedMode := binance.NewDegradedMode(binanceClient, storage, reducer, 2*time.Second)

	// 创建恢复管理器
	recoveryManager := recovery.NewRecoveryManager(storage, binanceClient, wsClient, reducer)

	// 设置WebSocket的外部组件引用
	wsClient.SetExternalComponents(degradedMode, recoveryManager)
	
	// 设置WebSocket的Reducer引用
	wsClient.SetReducer(reducer)

	// 创建监控器
	alertConfig := &monitoring.AlertConfig{
		ErrorRateThreshold:     0.1,                    // 10%错误率阈值
		ErrorRateWindow:        5 * time.Minute,       // 5分钟统计窗口
		LatencyThreshold:       2 * time.Second,       // 2秒延迟阈值
		LatencyWindow:          5 * time.Minute,       // 5分钟统计窗口
		LowBalanceThresholds:   map[string]float64{    // 低余额阈值
			"USDT": 100.0,  // USDT低于100时告警
			"BTC":  0.001,  // BTC低于0.001时告警
		},
		BalanceChangeThreshold: 1000.0,                // 余额变化1000以上告警
		AlertCooldown:          5 * time.Minute,       // 5分钟告警冷却
	}
	monitor := monitoring.NewMonitor(alertConfig)
	
	// 为不同资产配置不同的防抖参数
	// USDT配置 - 较大的绝对阈值，较小的百分比阈值
	usdtDebounceConfig := &monitoring.DebounceConfig{
		PercentageThreshold: 0.0001,                 // 0.01%变化阈值
		AbsoluteThreshold:   0.01,                   // 0.01 USDT绝对值阈值
		TimeWindow:          200 * time.Millisecond, // 200ms时间窗口
		MaxDelay:            3 * time.Second,        // 最大延迟3秒
	}
	monitor.GetBalanceMonitor().SetDebounceConfig("USDT", usdtDebounceConfig)
	
	// BTC配置 - 较小的绝对阈值，较大的百分比阈值
	btcDebounceConfig := &monitoring.DebounceConfig{
		PercentageThreshold: 0.001,                  // 0.1%变化阈值
		AbsoluteThreshold:   0.00000001,             // 0.00000001 BTC绝对值阈值
		TimeWindow:          500 * time.Millisecond, // 500ms时间窗口
		MaxDelay:            5 * time.Second,        // 最大延迟5秒
	}
	monitor.GetBalanceMonitor().SetDebounceConfig("BTC", btcDebounceConfig)
	
	// ETH配置
	ethDebounceConfig := &monitoring.DebounceConfig{
		PercentageThreshold: 0.001,                  // 0.1%变化阈值
		AbsoluteThreshold:   0.000001,               // 0.000001 ETH绝对值阈值
		TimeWindow:          300 * time.Millisecond, // 300ms时间窗口
		MaxDelay:            4 * time.Second,        // 最大延迟4秒
	}
	monitor.GetBalanceMonitor().SetDebounceConfig("ETH", ethDebounceConfig)
	
	// 设置监控器到Reducer
	reducer.SetMonitor(monitor)
	
	// 设置告警回调
	monitor.SetAlertCallback(func(alert *monitoring.Alert) {
		log.Printf("ALERT [%s] %s: %s", alert.Level, alert.Title, alert.Message)
		// 这里可以添加更多告警处理逻辑，如发送邮件、短信等
	})
	
	return &GridTrader{
		config:           cfg,
		storage:          storage,
		binanceClient:    binanceClient,
		wsClient:         wsClient,
		errorHandler:     errorHandler,
		pairingEngine:    pairingEngine,
		reducer:          reducer,
		degradedMode:     degradedMode,
		recoveryManager:  recoveryManager,
		multiGridManager: nil, // 将在Start方法中初始化
		monitor:          monitor,
		ctx:              context.Background(),
	}, nil
}

// setupCallbacks 设置回调函数
func (gt *GridTrader) setupCallbacks() {
	// 设置错误处理回调
	gt.errorHandler.SetCallbacks(
		gt.onTradingError,
		gt.onRateLimit,
		gt.onInsufficientBalance,
	)
	
	// 设置配对引擎回调
	gt.pairingEngine.SetCallbacks(
		gt.onPairCreated,
		gt.onPairCompleted,
		gt.onPartialFill,
	)
	
	// 设置多网格管理器回调
	gt.multiGridManager.SetCallbacks(
		gt.onOrderUpdate,
		gt.onExecution,
		gt.onGridError,
	)
	
	// 设置WebSocket回调
	gt.wsClient.SetCallbacks(
		gt.onWSStateChange,
		gt.onWSOrderUpdate,
		gt.onWSAccountUpdate,
		gt.onWSMarkPrice,
	)
	
	// 设置降级模式回调
	gt.degradedMode.SetCallbacks(
		gt.onOrderUpdate,
		gt.onAccountUpdate,
		nil, // 价格更新暂时不需要
	)
}

// Start 启动交易器
func (gt *GridTrader) Start() error {
	log.Println("Starting grid trader components...")
	
	// 0. 初始化交易所信息和精度规则
	log.Println("Fetching exchange info and trading rules...")
	if _, err := gt.binanceClient.GetExchangeInfo(); err != nil {
		return fmt.Errorf("failed to fetch exchange info: %w", err)
	}
	log.Println("Exchange info loaded successfully")
	
	// 1. 执行启动恢复
	log.Println("Starting recovery process...")
	if err := gt.recoveryManager.StartRecovery(); err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}
	
	// 打印恢复统计
	stats := gt.recoveryManager.GetRecoveryStats()
	log.Printf("Recovery completed: %+v", stats)
	
	// 2. 启动WebSocket连接 - 使用用户数据流
	log.Println("Creating listenKey for user data stream...")
	listenKeyResp, err := gt.binanceClient.CreateListenKey()
	if err != nil {
		log.Printf("Create listenKey failed: %v, entering degraded mode", err)
		gt.systemState = types.SystemStateDegraded
	} else {
		log.Printf("Created listenKey: %s", listenKeyResp.ListenKey)
		log.Println("Starting WebSocket user data stream connection...")
		if err := gt.wsClient.ConnectUserDataStream(listenKeyResp.ListenKey); err != nil {
			log.Printf("WebSocket user data stream connection failed, entering degraded mode: %v", err)
			gt.systemState = types.SystemStateDegraded
		} else {
			log.Println("WebSocket user data stream connected successfully")
		}
	}
	
	// 2.5. 初始化多网格管理器
	log.Println("Initializing multi-grid manager...")
	gt.multiGridManager = multigrid.NewMultiGridManager(
		gt.binanceClient,
		gt.wsClient,
		gt.storage,
		gt.pairingEngine,
	)
	
	// 3. 创建默认网格配置
	log.Println("Creating default grid configurations...")
	if err := gt.createDefaultGrids(); err != nil {
		return fmt.Errorf("failed to create default grids: %w", err)
	}
	
	// 4. 启动监控器
	if err := gt.monitor.Start(); err != nil {
		return fmt.Errorf("failed to start monitor: %w", err)
	}
	
	// 5. 启动多网格管理器
	log.Println("Starting multi-grid manager...")
	if err := gt.multiGridManager.Start(); err != nil {
		return fmt.Errorf("failed to start multi-grid manager: %w", err)
	}
	
	// 5.5. 配置降级模式的交易对列表
	log.Println("Configuring degraded mode symbols...")
	symbols := gt.getActiveSymbols()
	gt.degradedMode.SetSymbols(symbols)
	log.Printf("Degraded mode configured with symbols: %v", symbols)
	
	// 6. 启动监控协程
	go gt.monitorSystem()
	
	gt.isRunning = true
	log.Println("Grid trader started successfully")
	
	return nil
}

// getActiveSymbols 获取当前活跃的交易对列表
func (gt *GridTrader) getActiveSymbols() []string {
	symbols := make([]string, 0)
	
	// 从配置中获取主要交易对
	if gt.config.Trading.Symbol != "" {
		symbols = append(symbols, gt.config.Trading.Symbol)
	}
	
	// 从多网格管理器获取所有网格的交易对
	if gt.multiGridManager != nil {
		gridConfigs := gt.multiGridManager.GetGridConfigs()
		symbolSet := make(map[string]bool)
		
		for _, config := range gridConfigs {
			if config.Symbol != "" && !symbolSet[config.Symbol] {
				symbols = append(symbols, config.Symbol)
				symbolSet[config.Symbol] = true
			}
		}
	}
	
	// 如果没有配置任何交易对，使用默认值
	if len(symbols) == 0 {
		log.Println("No symbols configured, using default BTCUSDT")
		symbols = append(symbols, "BTCUSDT")
	}
	
	return symbols
}

// createDefaultGrids 创建默认网格配置
func (gt *GridTrader) createDefaultGrids() error {
	// 创建网格配置
	if err := gt.multiGridManager.CreateDefaultGrids(gt.config.Trading.Symbol, &gt.config.Trading.Grids); err != nil {
		return fmt.Errorf("failed to create default grids: %w", err)
	}
	
	return nil
}

// monitorSystem 监控系统状态
func (gt *GridTrader) monitorSystem() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-gt.ctx.Done():
			return
			
		case <-ticker.C:
			gt.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (gt *GridTrader) performHealthCheck() {
	// 检查WebSocket连接状态
	if !gt.wsClient.IsConnected() && gt.systemState == types.SystemStateNormal {
		log.Println("WebSocket disconnected, entering degraded mode")
		gt.systemState = types.SystemStateDegraded
	} else if gt.wsClient.IsConnected() && gt.systemState == types.SystemStateDegraded {
		log.Println("WebSocket reconnected, returning to normal mode")
		gt.systemState = types.SystemStateNormal
	}
	
	// 检查多网格管理器状态
	if !gt.multiGridManager.IsRunning() {
		log.Println("Multi-grid manager stopped, attempting restart")
		if err := gt.multiGridManager.Start(); err != nil {
			log.Printf("Failed to restart multi-grid manager: %v", err)
		}
	}
	
	// 打印统计信息
	gt.printStats()
}

// printStats 打印统计信息
func (gt *GridTrader) printStats() {
	// 获取网格统计
	gridStats := gt.multiGridManager.GetGridStats()
	
	// 获取配对统计
	pairingStats := gt.pairingEngine.GetPairStats()
	
	// 获取错误统计
	errorStats := gt.errorHandler.GetErrorStats()
	
	log.Printf("=== System Status ===")
	log.Printf("System State: %s", gt.systemState)
	log.Printf("WebSocket Connected: %v", gt.wsClient.IsConnected())
	
	// 安全地访问统计数据
	if grids, exists := gridStats["grids"]; exists {
		if gridMap, ok := grids.(map[string]interface{}); ok {
			log.Printf("Active Grids: %d", len(gridMap))
		}
	}
	if totalGrids, exists := gridStats["total_grids"]; exists {
		log.Printf("Total Grids: %v", totalGrids)
	}
	if runningGrids, exists := gridStats["running_grids"]; exists {
		log.Printf("Running Grids: %v", runningGrids)
	}
	
	// 安全地访问配对统计数据
	if completedPairs, exists := pairingStats["completed_pairs"]; exists {
		log.Printf("Total Completed Pairs: %v", completedPairs)
	}
	if totalPnl, exists := pairingStats["total_pnl"]; exists {
		log.Printf("Total P&L: %v USDT", totalPnl)
	}
	if activePairs, exists := pairingStats["active_pairs"]; exists {
		log.Printf("Active Pairs: %v", activePairs)
	}
	
	if len(errorStats) > 0 {
		log.Printf("Error Stats: %+v", errorStats)
	}
	
	// 打印监控统计
	stats := gt.monitor.GetStats()
	if errorStats, ok := stats["error_rate"].(map[string]interface{}); ok {
		log.Printf("Error Rate: %.2f%%, Success Rate: %.2f%%", 
			errorStats["error_rate"].(float64)*100,
			errorStats["success_rate"].(float64)*100)
	}
	
	if latencyStats, ok := stats["latency"].(map[string]interface{}); ok {
		log.Printf("Avg Latency: %dms, P95 Latency: %dms", 
			latencyStats["avg_latency_ms"],
			latencyStats["p95_latency_ms"])
	}
	
	if balanceStats, ok := stats["balance"].(map[string]interface{}); ok {
		if balances, ok := balanceStats["balances"].(map[string]float64); ok {
			for asset, balance := range balances {
				log.Printf("Balance %s: %.6f", asset, balance)
			}
		}
	}
	
	log.Printf("====================")
}

// Stop 停止交易器
func (gt *GridTrader) Stop() error {
	if !gt.isRunning {
		return nil
	}
	
	log.Println("Stopping grid trader...")
	
	// 停止WebSocket连接
	if gt.wsClient != nil {
		if err := gt.wsClient.Close(); err != nil {
			log.Printf("Error closing WebSocket: %v", err)
		}
	}
	
	// 停止监控器
	if gt.monitor != nil {
		if err := gt.monitor.Stop(); err != nil {
			log.Printf("Error stopping monitor: %v", err)
		}
	}
	
	// 停止多网格管理器
	if gt.multiGridManager != nil {
		gt.multiGridManager.Stop()
	}
	
	// 停止配对引擎
	if gt.pairingEngine != nil {
		// PairingEngine没有Stop方法，只需要清理资源
		log.Println("Pairing engine cleanup completed")
	}
	
	// 取消上下文
	if gt.cancel != nil {
		gt.cancel()
	}
	gt.isRunning = false
	
	log.Println("Grid trader stopped")
	return nil
}

// WaitForShutdown 等待关闭信号
func (gt *GridTrader) WaitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	log.Println("Received shutdown signal")
	
	if err := gt.Stop(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}

// 回调函数实现

// onTradingError 交易错误回调
func (gt *GridTrader) onTradingError(err *types.TradingError) {
	log.Printf("Trading error: %s - %s", err.Code, err.Message)
	
	// 记录错误到监控系统
	gt.monitor.RecordRequest(false, 0, err.Code)
	
	// 根据错误类型执行特定操作
	switch err.Code {
	case types.ErrorCodeRateLimit:
		// 速率限制时暂停操作
		log.Println("Rate limit hit, pausing operations")
		
	case types.ErrorCodeInsufficientBalance:
		// 余额不足时停止相关网格
		log.Println("Insufficient balance, may need to stop some grids")
		
	case types.ErrorCodeNetworkError:
		// 网络错误时切换到降级模式
		if gt.systemState == types.SystemStateNormal {
			gt.systemState = types.SystemStateDegraded
			log.Println("Network error, switching to degraded mode")
			// 暂停所有网格并启用只平仓模式
			if gt.multiGridManager != nil {
				gt.multiGridManager.PauseAllGrids()
				gt.multiGridManager.SetCloseOnlyModeForAllGrids(true)
			}
		}
	}
}

// onRateLimit 速率限制回调
func (gt *GridTrader) onRateLimit(waitTime time.Duration) {
	log.Printf("Rate limit triggered, waiting %v", waitTime)
}

// onInsufficientBalance 余额不足回调
func (gt *GridTrader) onInsufficientBalance(asset string, required, available float64) {
	log.Printf("Insufficient balance: %s, required: %.6f, available: %.6f", 
		asset, required, available)
}

// onPairCreated 配对创建回调
func (gt *GridTrader) onPairCreated(pair *types.OrderPair) {
	log.Printf("Pair created: %s, Buy: %s, Sell: %s", 
		pair.GridLegID, pair.BuyOrderID, pair.SellOrderID)
}

// onPairCompleted 配对完成回调
func (gt *GridTrader) onPairCompleted(pair *types.OrderPair) {
	log.Printf("Pair completed: %s, Profit: %.6f USDT", 
		pair.GridLegID, pair.ProfitLoss)
}

// onPartialFill 部分成交回调
func (gt *GridTrader) onPartialFill(clientOrderID string, execution *types.Execution) {
	log.Printf("Partial fill: Order %s, Price: %.6f, Qty: %.6f", 
		clientOrderID, execution.Price, execution.Qty)
}

// onOrderUpdate 订单更新回调
func (gt *GridTrader) onOrderUpdate(order *types.Order) {
	log.Printf("Order update: %s, Status: %s, Price: %.6f, Qty: %.6f", 
		order.OrderID, order.Status, order.Price, order.OrigQty)
}

// onExecution 执行回调
func (gt *GridTrader) onExecution(clientOrderID string, execution *types.Execution) {
	log.Printf("Execution: Order %s, Price: %.6f, Qty: %.6f", 
		clientOrderID, execution.Price, execution.Qty)
}

// onAccountUpdate 账户更新回调
func (gt *GridTrader) onAccountUpdate(account *types.AccountInfo) {
	// 将字符串转换为float64
	totalBalance, err := strconv.ParseFloat(account.TotalWalletBalance, 64)
	if err != nil {
		log.Printf("Error parsing total wallet balance: %v", err)
		return
	}
	
	log.Printf("Account update: Total balance: %.6f USDT", totalBalance)
	
	// 更新余额监控
	gt.monitor.UpdateBalance("USDT", totalBalance)
	
	// 更新其他资产余额
	for _, balance := range account.Balances {
		// 解析WalletBalance字符串
		walletBalance, err := strconv.ParseFloat(balance.WalletBalance, 64)
		if err != nil {
			log.Printf("Error parsing wallet balance for %s: %v", balance.Asset, err)
			continue
		}
		
		if walletBalance > 0 {
			gt.monitor.UpdateBalance(balance.Asset, walletBalance)
		}
	}
}

// onGridError 网格错误回调
func (gt *GridTrader) onGridError(err error) {
	log.Printf("Grid error: %v", err)
}

// WebSocket回调函数

// onWSStateChange WebSocket状态变化回调
func (gt *GridTrader) onWSStateChange(state binance.SystemState) {
	log.Printf("WebSocket state changed: %s", state)
	switch state {
	case binance.SystemStateDegraded:
		log.Println("Switching to degraded mode")
		// 暂停所有网格并启用只平仓模式
		if gt.multiGridManager != nil {
			gt.multiGridManager.PauseAllGrids()
			gt.multiGridManager.SetCloseOnlyModeForAllGrids(true)
		}
		gt.degradedMode.Start()
	case binance.SystemStateRecovering:
		log.Println("Entering recovery mode - performing light reconciliation")
		// WebSocket恢复后，强制一次轻对账
		if gt.recoveryManager != nil {
			gt.recoveryManager.ReconcileOnce()
		}
		// 对账完成后切换到正常模式
		log.Println("Light reconciliation completed, switching to normal mode")
		gt.systemState = types.SystemStateNormal
	case binance.SystemStateNormal:
		log.Println("Switching back to normal mode")
		// 恢复所有网格并禁用只平仓模式
		if gt.multiGridManager != nil {
			gt.multiGridManager.ResumeAllGrids()
			gt.multiGridManager.SetCloseOnlyModeForAllGrids(false)
		}
		gt.degradedMode.Stop()
	}
}

// onWSOrderUpdate WebSocket订单更新回调
func (gt *GridTrader) onWSOrderUpdate(event *binance.OrderUpdateEvent) {
	// 转换为内部Order类型并处理
	log.Printf("WS Order update: %s, Status: %s", event.ClientOrderID, event.OrderStatus)
}

// onWSAccountUpdate WebSocket账户更新回调
func (gt *GridTrader) onWSAccountUpdate(event *binance.AccountUpdateEvent) {
	log.Printf("WS Account update received, reason: %s", event.AccountData.Reason)
	
	// 处理余额更新
	for _, balance := range event.AccountData.Balances {
		// 解析WalletBalance字符串
		walletBalance, err := strconv.ParseFloat(balance.WalletBalance, 64)
		if err != nil {
			log.Printf("Error parsing wallet balance for %s: %v", balance.Asset, err)
			continue
		}
		
		if walletBalance > 0 {
			log.Printf("WS Balance update: %s = %.6f", balance.Asset, walletBalance)
			gt.monitor.UpdateBalance(balance.Asset, walletBalance)
		}
	}
}

// onWSMarkPrice WebSocket标记价格更新回调
func (gt *GridTrader) onWSMarkPrice(event *binance.MarkPriceEvent) {
	log.Printf("WS Mark price update: %s = %s", event.Symbol, event.MarkPrice)
}
package multigrid

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"binance-grid-trader/binance"
	"binance-grid-trader/config"
	"binance-grid-trader/grid"
	"binance-grid-trader/pairing"
	"binance-grid-trader/storage"
	"binance-grid-trader/types"
)

// MultiGridManager 多网格管理器
type MultiGridManager struct {
	mu             sync.RWMutex
	client         *binance.Client
	wsClient       *binance.WSClient
	storage        *storage.Storage
	pairingEngine  *pairing.PairingEngine
	
	// 网格引擎
	grids          map[string]*grid.Engine // gridID -> engine
	gridConfigs    map[string]*types.MultiGridConfig
	
	// 状态管理
	isRunning      bool
	stopCh         chan struct{}
	
	// 回调函数
	onOrderUpdate  func(*types.Order)
	onExecution    func(string, *types.Execution)
	onError        func(error)
}

// NewMultiGridManager 创建多网格管理器
func NewMultiGridManager(
	client *binance.Client,
	wsClient *binance.WSClient,
	storage *storage.Storage,
	pairingEngine *pairing.PairingEngine,
) *MultiGridManager {
	return &MultiGridManager{
		client:        client,
		wsClient:      wsClient,
		storage:       storage,
		pairingEngine: pairingEngine,
		grids:         make(map[string]*grid.Engine),
		gridConfigs:   make(map[string]*types.MultiGridConfig),
		stopCh:        make(chan struct{}),
	}
}

// SetCallbacks 设置回调函数
func (mgm *MultiGridManager) SetCallbacks(
	onOrderUpdate func(*types.Order),
	onExecution func(string, *types.Execution),
	onError func(error),
) {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	mgm.onOrderUpdate = onOrderUpdate
	mgm.onExecution = onExecution
	mgm.onError = onError
}

// AddGrid 添加网格配置
func (mgm *MultiGridManager) AddGrid(config *types.MultiGridConfig) error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	if _, exists := mgm.gridConfigs[config.GridID]; exists {
		return fmt.Errorf("grid %s already exists", config.GridID)
	}
	
	// 验证配置
	if err := mgm.validateGridConfig(config); err != nil {
		return fmt.Errorf("invalid grid config: %w", err)
	}
	
	// 创建网格引擎
	gridEngine, err := mgm.createGridEngine(config)
	if err != nil {
		return fmt.Errorf("failed to create grid engine: %w", err)
	}
	
	mgm.gridConfigs[config.GridID] = config
	mgm.grids[config.GridID] = gridEngine
	
	log.Printf("Added grid: %s, Symbol: %s, Type: %s, Levels: %d", 
		config.GridID, config.Symbol, config.GridType, config.GridLevels)
	
	// 如果管理器正在运行，启动新网格
	if mgm.isRunning {
		// 获取实时价格
		currentPrice, err := mgm.getCurrentPrice(config.Symbol)
		if err != nil {
			log.Printf("Failed to get current price for %s, using fallback: %v", config.Symbol, err)
			currentPrice = 1000.0 // 使用默认价格作为后备
		}
		
		if err := gridEngine.Start(currentPrice); err != nil {
			log.Printf("Failed to start grid %s: %v", config.GridID, err)
		}
	}
	
	return nil
}

// RemoveGrid 移除网格
func (mgm *MultiGridManager) RemoveGrid(gridID string) error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	gridEngine, exists := mgm.grids[gridID]
	if !exists {
		return fmt.Errorf("grid %s not found", gridID)
	}
	
	// 停止网格引擎
	if err := gridEngine.Stop(); err != nil {
		log.Printf("Failed to stop grid %s: %v", gridID, err)
	}
	
	// 取消所有未完成订单
	if err := mgm.cancelGridOrders(gridID); err != nil {
		log.Printf("Failed to cancel orders for grid %s: %v", gridID, err)
	}
	
	delete(mgm.grids, gridID)
	delete(mgm.gridConfigs, gridID)
	
	log.Printf("Removed grid: %s", gridID)
	return nil
}

// Start 启动多网格管理器
func (mgm *MultiGridManager) Start() error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	if mgm.isRunning {
		return fmt.Errorf("multi-grid manager already running")
	}
	
	log.Println("Starting multi-grid manager...")
	
	// 启动所有网格引擎
	for gridID, gridEngine := range mgm.grids {
		config := mgm.gridConfigs[gridID]
		
		// 获取实时价格
		currentPrice, err := mgm.getCurrentPrice(config.Symbol)
		if err != nil {
			log.Printf("Failed to get current price for %s, skipping grid start: %v", config.Symbol, err)
			continue
		}
		
		if err := gridEngine.Start(currentPrice); err != nil {
			log.Printf("Failed to start grid %s: %v", gridID, err)
			continue
		}
		log.Printf("Started grid: %s with price: %.2f", gridID, currentPrice)
	}
	
	// 启动监控协程
	go mgm.monitorGrids()
	
	mgm.isRunning = true
	log.Println("Multi-grid manager started successfully")
	
	return nil
}

// PauseAllGrids 暂停所有网格
func (mgm *MultiGridManager) PauseAllGrids() error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	log.Println("Pausing all grids due to degraded mode")
	
	for gridID, gridEngine := range mgm.grids {
		if err := gridEngine.Pause(); err != nil {
			log.Printf("Failed to pause grid %s: %v", gridID, err)
		} else {
			log.Printf("Paused grid: %s", gridID)
		}
	}
	
	return nil
}

// ResumeAllGrids 恢复所有网格
func (mgm *MultiGridManager) ResumeAllGrids() error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	log.Println("Resuming all grids after recovery")
	
	for gridID, gridEngine := range mgm.grids {
		if err := gridEngine.Resume(); err != nil {
			log.Printf("Failed to resume grid %s: %v", gridID, err)
		} else {
			log.Printf("Resumed grid: %s", gridID)
		}
	}
	
	return nil
}

// SetCloseOnlyModeForAllGrids 为所有网格设置只平仓模式
func (mgm *MultiGridManager) SetCloseOnlyModeForAllGrids(enabled bool) error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	action := "Enabling"
	if !enabled {
		action = "Disabling"
	}
	log.Printf("%s close-only mode for all grids", action)
	
	for gridID, gridEngine := range mgm.grids {
		if err := gridEngine.SetCloseOnlyMode(enabled); err != nil {
			log.Printf("Failed to set close-only mode for grid %s: %v", gridID, err)
		} else {
			log.Printf("Set close-only mode (%v) for grid: %s", enabled, gridID)
		}
	}
	
	return nil
}

// Stop 停止多网格管理器
func (mgm *MultiGridManager) Stop() error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	if !mgm.isRunning {
		return nil
	}
	
	log.Println("Stopping multi-grid manager...")
	
	// 发送停止信号
	close(mgm.stopCh)
	
	// 停止所有网格引擎
	for gridID, gridEngine := range mgm.grids {
		if err := gridEngine.Stop(); err != nil {
			log.Printf("Failed to stop grid %s: %v", gridID, err)
		} else {
			log.Printf("Stopped grid: %s", gridID)
		}
	}
	
	mgm.isRunning = false
	log.Println("Multi-grid manager stopped")
	
	return nil
}

// ProcessOrderUpdate 处理订单更新
func (mgm *MultiGridManager) ProcessOrderUpdate(order *types.Order) error {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	// 根据GridLegID找到对应的网格
	gridID := mgm.extractGridIDFromLegID(order.GridLegID)
	if gridID == "" {
		log.Printf("Cannot extract grid ID from GridLegID: %s", order.GridLegID)
		return nil
	}
	
	gridEngine, exists := mgm.grids[gridID]
	if !exists {
		log.Printf("Grid %s not found for order %s", gridID, order.ClientOrderID)
		return nil
	}
	
	// 处理订单更新
	if err := gridEngine.ProcessOrderUpdate(order); err != nil {
		log.Printf("Failed to process order update in grid %s: %v", gridID, err)
	}
	
	// 处理配对逻辑
	if err := mgm.pairingEngine.ProcessOrderUpdate(order); err != nil {
		log.Printf("Failed to process pairing for order %s: %v", order.ClientOrderID, err)
	}
	
	// 触发回调
	if mgm.onOrderUpdate != nil {
		mgm.onOrderUpdate(order)
	}
	
	return nil
}

// ProcessExecution 处理成交记录
func (mgm *MultiGridManager) ProcessExecution(clientOrderID string, execution *types.Execution) error {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	// 处理配对逻辑
	if err := mgm.pairingEngine.ProcessExecution(clientOrderID, execution); err != nil {
		log.Printf("Failed to process execution pairing: %v", err)
	}
	
	// 触发回调
	if mgm.onExecution != nil {
		mgm.onExecution(clientOrderID, execution)
	}
	
	return nil
}

// GetGridStats 获取网格统计信息
func (mgm *MultiGridManager) GetGridStats() map[string]interface{} {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	stats := make(map[string]interface{})
	
	// 总体统计
	stats["total_grids"] = len(mgm.grids)
	stats["running_grids"] = 0
	
	// 各网格统计
	gridStats := make(map[string]interface{})
	
	for gridID, gridEngine := range mgm.grids {
		config := mgm.gridConfigs[gridID]
		
		gridInfo := map[string]interface{}{
			"grid_id":     gridID,
			"symbol":      config.Symbol,
			"grid_type":   config.GridType,
			"grid_levels": config.GridLevels,
			"is_running":  gridEngine.IsRunning(),
			"price_range": map[string]float64{
				"lower": config.LowerPrice,
				"upper": config.UpperPrice,
			},
		}
		
		if gridEngine.IsRunning() {
			stats["running_grids"] = stats["running_grids"].(int) + 1
		}
		
		gridStats[gridID] = gridInfo
	}
	
	stats["grids"] = gridStats
	
	// 配对统计
	pairStats := mgm.pairingEngine.GetPairStats()
	stats["pairing"] = pairStats
	
	return stats
}

// GetGridByID 根据ID获取网格引擎
func (mgm *MultiGridManager) GetGridByID(gridID string) (*grid.Engine, bool) {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	engine, exists := mgm.grids[gridID]
	return engine, exists
}

// GetAllGrids 获取所有网格引擎
func (mgm *MultiGridManager) GetAllGrids() map[string]*grid.Engine {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	result := make(map[string]*grid.Engine)
	for gridID, engine := range mgm.grids {
		result[gridID] = engine
	}
	
	return result
}

// UpdateGridConfig 更新网格配置
func (mgm *MultiGridManager) UpdateGridConfig(gridID string, config *types.MultiGridConfig) error {
	mgm.mu.Lock()
	defer mgm.mu.Unlock()
	
	if config.GridID != gridID {
		return fmt.Errorf("grid ID mismatch: %s != %s", config.GridID, gridID)
	}
	
	gridEngine, exists := mgm.grids[gridID]
	if !exists {
		return fmt.Errorf("grid %s not found", gridID)
	}
	
	// 验证新配置
	if err := mgm.validateGridConfig(config); err != nil {
		return fmt.Errorf("invalid grid config: %w", err)
	}
	
	// 停止当前网格
	if err := gridEngine.Stop(); err != nil {
		log.Printf("Failed to stop grid %s for update: %v", gridID, err)
	}
	
	// 更新配置
	mgm.gridConfigs[gridID] = config
	
	// 重新创建网格引擎
	newGridEngine, err := mgm.createGridEngine(config)
	if err != nil {
		return fmt.Errorf("failed to create new grid engine: %w", err)
	}
	
	mgm.grids[gridID] = newGridEngine
	
	// 如果管理器正在运行，启动更新后的网格
	if mgm.isRunning {
		// 获取实时价格
		currentPrice, err := mgm.getCurrentPrice(config.Symbol)
		if err != nil {
			log.Printf("Failed to get current price for %s, skipping grid update: %v", config.Symbol, err)
			return fmt.Errorf("failed to get current price for %s: %w", config.Symbol, err)
		}
		
		if err := newGridEngine.Start(currentPrice); err != nil {
			log.Printf("Failed to start updated grid %s: %v", gridID, err)
		}
	}
	
	log.Printf("Updated grid config: %s", gridID)
	return nil
}

// validateGridConfig 验证网格配置
func (mgm *MultiGridManager) validateGridConfig(config *types.MultiGridConfig) error {
	if config.GridID == "" {
		return fmt.Errorf("grid ID cannot be empty")
	}
	
	if config.Symbol == "" {
		return fmt.Errorf("symbol cannot be empty")
	}
	
	if config.GridLevels <= 0 {
		return fmt.Errorf("grid levels must be positive")
	}
	
	if config.LowerPrice <= 0 || config.UpperPrice <= 0 {
		return fmt.Errorf("price range must be positive")
	}
	
	if config.LowerPrice >= config.UpperPrice {
		return fmt.Errorf("lower price must be less than upper price")
	}
	
	if config.GridSpacing <= 0 {
		return fmt.Errorf("grid spacing must be positive")
	}
	
	if config.OrderQuantity <= 0 {
		return fmt.Errorf("order quantity must be positive")
	}
	
	// 验证网格类型
	switch config.GridType {
	case "X_SEGMENT", "A_SEGMENT", "B_SEGMENT":
		// 有效的网格类型
	default:
		return fmt.Errorf("invalid grid type: %s", config.GridType)
	}
	
	return nil
}

// createGridEngine 创建网格引擎
func (mgm *MultiGridManager) createGridEngine(config *types.MultiGridConfig) (*grid.Engine, error) {
	// 转换为网格引擎配置
	gridConfig := &types.GridConfig{
		Symbol:        config.Symbol,
		PriceMin:      config.LowerPrice,
		PriceMax:      config.UpperPrice,
		StepSize:      (config.UpperPrice - config.LowerPrice) / float64(config.GridLevels),
		OrderQty:      100.0, // 默认订单数量
		MaxBuyOrders:  config.GridLevels / 2,
		MaxSellOrders: config.GridLevels / 2,
		Levels:        []types.GridLevel{}, // 初始化为空切片
	}
	
	// 创建网格引擎
	engine := grid.NewEngine(gridConfig)
	
	// 设置网格ID前缀
	engine.SetGridIDPrefix(config.GridID)
	
	return engine, nil
}

// extractGridIDFromLegID 从GridLegID提取网格ID
func (mgm *MultiGridManager) extractGridIDFromLegID(gridLegID string) string {
	if gridLegID == "" {
		return ""
	}
	
	// GridLegID格式: {GridID}_{Symbol}_{Price}_{Timestamp}
	// 提取第一部分作为GridID
	for i, char := range gridLegID {
		if char == '_' {
			return gridLegID[:i]
		}
	}
	
	return gridLegID // 如果没有下划线，整个字符串就是GridID
}

// cancelGridOrders 取消网格的所有订单
func (mgm *MultiGridManager) cancelGridOrders(gridID string) error {
	// 获取所有活跃订单
	orderTable := mgm.storage.GetOrderTable()
	orders := orderTable.GetAll()
	
	var cancelErrors []error
	
	for _, order := range orders {
		// 检查是否属于该网格
		if mgm.extractGridIDFromLegID(order.GridLegID) == gridID {
			// 只取消活跃状态的订单
			if order.Status.IsActive() {
				cancelReq := &binance.CancelOrderRequest{
					Symbol:            order.Symbol,
					OrigClientOrderID: order.ClientOrderID,
				}
				if _, err := mgm.client.CancelOrder(cancelReq); err != nil {
					cancelErrors = append(cancelErrors, fmt.Errorf("failed to cancel order %s: %w", order.ClientOrderID, err))
				} else {
					log.Printf("Canceled order %s for grid %s", order.ClientOrderID, gridID)
				}
			}
		}
	}
	
	if len(cancelErrors) > 0 {
		return fmt.Errorf("failed to cancel some orders: %v", cancelErrors)
	}
	
	return nil
}

// monitorGrids 监控网格状态
func (mgm *MultiGridManager) monitorGrids() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			mgm.performGridHealthCheck()
		case <-mgm.stopCh:
			return
		}
	}
}

// performGridHealthCheck 执行网格健康检查
func (mgm *MultiGridManager) performGridHealthCheck() {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	for gridID, gridEngine := range mgm.grids {
		// 检查网格引擎状态
		if !gridEngine.IsRunning() {
			log.Printf("Grid %s is not running, attempting to restart", gridID)
			
			config := mgm.gridConfigs[gridID]
			
			// 获取实时价格
			currentPrice, err := mgm.getCurrentPrice(config.Symbol)
			if err != nil {
				log.Printf("Failed to get current price for %s, skipping grid restart: %v", config.Symbol, err)
				continue
			}
			
			if err := gridEngine.Start(currentPrice); err != nil {
				log.Printf("Failed to restart grid %s: %v", gridID, err)
				if mgm.onError != nil {
					mgm.onError(fmt.Errorf("grid %s restart failed: %w", gridID, err))
				}
			} else {
				log.Printf("Successfully restarted grid %s with price: %.2f", gridID, currentPrice)
			}
		}
	}
}

// GetActiveOrders 获取所有活跃订单
func (mgm *MultiGridManager) GetActiveOrders() []*types.Order {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	orderTable := mgm.storage.GetOrderTable()
	orders := orderTable.GetAll()
	
	var activeOrders []*types.Order
	for _, order := range orders {
		if order.Status.IsActive() {
			activeOrders = append(activeOrders, order)
		}
	}
	
	return activeOrders
}

// GetOrdersByGrid 根据网格ID获取订单
func (mgm *MultiGridManager) GetOrdersByGrid(gridID string) []*types.Order {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	orderTable := mgm.storage.GetOrderTable()
	orders := orderTable.GetAll()
	
	var gridOrders []*types.Order
	for _, order := range orders {
		if mgm.extractGridIDFromLegID(order.GridLegID) == gridID {
			gridOrders = append(gridOrders, order)
		}
	}
	
	return gridOrders
}

// IsRunning 检查管理器是否正在运行
func (mgm *MultiGridManager) IsRunning() bool {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	return mgm.isRunning
}

// GetGridConfigs 获取所有网格配置
func (mgm *MultiGridManager) GetGridConfigs() map[string]*types.MultiGridConfig {
	mgm.mu.RLock()
	defer mgm.mu.RUnlock()
	
	result := make(map[string]*types.MultiGridConfig)
	for gridID, config := range mgm.gridConfigs {
		// 创建配置副本
		configCopy := *config
		result[gridID] = &configCopy
	}
	
	return result
}

// CreateDefaultGrids 根据配置创建网格
func (mgm *MultiGridManager) CreateDefaultGrids(symbol string, config *config.GridsConfig) error {
	// 添加调试输出
	log.Printf("DEBUG: Creating grids with config - X enabled: %v, A enabled: %v, B enabled: %v", 
		config.XSegment.Enabled, config.ASegment.Enabled, config.BSegment.Enabled)
	
	// 只创建启用的网格
	if config.XSegment.Enabled {
		xSegmentConfig := &types.MultiGridConfig{
			GridID:        "X_SEGMENT",
			Symbol:        symbol,
			GridType:      "X_SEGMENT",
			LowerPrice:    config.XSegment.PriceMin,
			UpperPrice:    config.XSegment.PriceMax,
			GridLevels:    int((config.XSegment.PriceMax - config.XSegment.PriceMin) / config.XSegment.StepSize),
			GridSpacing:   config.XSegment.StepSize,
			OrderQuantity: config.XSegment.OrderQty,
		}
		
		if err := mgm.AddGrid(xSegmentConfig); err != nil {
			return fmt.Errorf("failed to add X segment grid: %w", err)
		}
		log.Printf("Created X segment grid: %s, Range: %.2f-%.2f, Step: %.2f", 
			symbol, config.XSegment.PriceMin, config.XSegment.PriceMax, config.XSegment.StepSize)
	}
	
	if config.ASegment.Enabled {
		aSegmentConfig := &types.MultiGridConfig{
			GridID:        "A_SEGMENT",
			Symbol:        symbol,
			GridType:      "A_SEGMENT",
			LowerPrice:    config.ASegment.PriceMin,
			UpperPrice:    config.ASegment.PriceMax,
			GridLevels:    int((config.ASegment.PriceMax - config.ASegment.PriceMin) / config.ASegment.StepSize),
			GridSpacing:   config.ASegment.StepSize,
			OrderQuantity: config.ASegment.OrderQty,
		}
		
		if err := mgm.AddGrid(aSegmentConfig); err != nil {
			return fmt.Errorf("failed to add A segment grid: %w", err)
		}
		log.Printf("Created A segment grid: %s, Range: %.2f-%.2f, Step: %.2f", 
			symbol, config.ASegment.PriceMin, config.ASegment.PriceMax, config.ASegment.StepSize)
	}
	
	if config.BSegment.Enabled {
		bSegmentConfig := &types.MultiGridConfig{
			GridID:        "B_SEGMENT",
			Symbol:        symbol,
			GridType:      "B_SEGMENT",
			LowerPrice:    config.BSegment.PriceMin,
			UpperPrice:    config.BSegment.PriceMax,
			GridLevels:    int((config.BSegment.PriceMax - config.BSegment.PriceMin) / config.BSegment.StepSize),
			GridSpacing:   config.BSegment.StepSize,
			OrderQuantity: config.BSegment.OrderQty,
		}
		
		if err := mgm.AddGrid(bSegmentConfig); err != nil {
			return fmt.Errorf("failed to add B segment grid: %w", err)
		}
		log.Printf("Created B segment grid: %s, Range: %.2f-%.2f, Step: %.2f", 
			symbol, config.BSegment.PriceMin, config.BSegment.PriceMax, config.BSegment.StepSize)
	}
	
	return nil
}

// getCurrentPrice 获取指定交易对的当前市场价格
func (mgm *MultiGridManager) getCurrentPrice(symbol string) (float64, error) {
	if mgm.client == nil {
		return 0, fmt.Errorf("binance client is nil")
	}
	
	ticker, err := mgm.client.GetTickerPrice(symbol)
	if err != nil {
		log.Printf("Failed to get ticker price for %s: %v", symbol, err)
		return 0, err
	}
	
	// 将字符串价格转换为float64
	price, err := strconv.ParseFloat(ticker.Price, 64)
	if err != nil {
		log.Printf("Failed to parse price %s for %s: %v", ticker.Price, symbol, err)
		return 0, err
	}
	
	return price, nil
}
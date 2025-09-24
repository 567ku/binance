package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"binance-grid-trader/types"
)

// Storage 存储管理器
type Storage struct {
	dataDir string
	mu      sync.RWMutex
	
	// 内存表
	orderTable     *types.OrderTable
	executionTable *types.ExecutionTable
	pairTable      *types.PairTable
	
	// 文件句柄
	ledgerFile *os.File
	
	// 快照管理
	lastSnapshot time.Time
	snapshotInterval time.Duration
	maxEventsBeforeSnapshot int
	eventCount int
	
	// 游标管理（轻对账）
	sinceCursor int64 // 上次快照后的事件游标
	lastSequence int64 // 最后处理的事件序列号
}

// NewStorage 创建存储管理器
func NewStorage(dataDir string) (*Storage, error) {
	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	// 创建内存表
	orderTable := types.NewOrderTable()
	executionTable := types.NewExecutionTable()
	pairTable := types.NewPairTable()
	
	// 打开ledger文件
	ledgerPath := filepath.Join(dataDir, "ledger.jsonl")
	ledgerFile, err := os.OpenFile(ledgerPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open ledger file: %w", err)
	}
	
	storage := &Storage{
		dataDir:                 dataDir,
		orderTable:              orderTable,
		executionTable:          executionTable,
		pairTable:               pairTable,
		ledgerFile:              ledgerFile,
		snapshotInterval:        5 * time.Minute,
		maxEventsBeforeSnapshot: 1000,
	}
	
	// 尝试从快照恢复
	if err := storage.loadSnapshot(); err != nil {
		log.Printf("Failed to load snapshot: %v", err)
	}
	
	// 重放ledger
	if err := storage.replayLedger(); err != nil {
		log.Printf("Failed to replay ledger: %v", err)
	}
	
	return storage, nil
}

// GetOrderTable 获取订单表
func (s *Storage) GetOrderTable() *types.OrderTable {
	return s.orderTable
}

// GetExecutionTable 获取执行表
func (s *Storage) GetExecutionTable() *types.ExecutionTable {
	return s.executionTable
}

// GetPairTable 获取配对表
func (s *Storage) GetPairTable() *types.PairTable {
	return s.pairTable
}

// SaveOrder 保存订单到内存表
func (s *Storage) SaveOrder(order *types.Order) error {
	s.orderTable.Add(order)
	
	// 写入事件日志
	event := types.Event{
		Timestamp: time.Now().UnixNano(),
		Type:      types.EventOrderPlaced,
		OrderID:   order.ClientOrderID,
		Side:      order.Side,
		Price:     order.Price,
		Quantity:  order.OrigQty,
		Source:    "STORAGE", // 标识来源为存储层
		LatencyMs: 0,         // 存储操作无网络延迟
	}
	
	return s.writeEvent(event)
}

// SaveExecution 保存成交记录
func (s *Storage) SaveExecution(execution *types.Execution) error {
	s.executionTable.Add(execution.OrderID, execution)
	
	// 写入事件日志
	event := types.Event{
		Timestamp: time.Now().UnixNano(),
		Type:      types.EventOrderFilled,
		OrderID:   execution.OrderID,
		Price:     execution.Price,
		Quantity:  execution.Qty,
		Source:    "STORAGE", // 标识来源为存储层
		LatencyMs: 0,         // 存储操作无网络延迟
	}
	
	return s.writeEvent(event)
}

// SavePair 保存配对信息
func (s *Storage) SavePair(pair *types.OrderPair) error {
	s.pairTable.Add(pair)
	
	// 写入事件日志
	event := types.Event{
		Timestamp: time.Now().UnixNano(),
		Type:      types.EventGridPaired,
		GridID:    pair.GridLegID,
		Price:     pair.Price,
		Quantity:  pair.Qty,
		Source:    "STORAGE", // 标识来源为存储层
		LatencyMs: 0,         // 存储操作无网络延迟
	}
	
	return s.writeEvent(event)
}

// writeLedgerEvent 写入ledger事件
func (s *Storage) writeLedgerEvent(event map[string]interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	
	if _, err := s.ledgerFile.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to ledger: %w", err)
	}
	
	s.eventCount++
	
	// 检查是否需要创建快照
	if s.shouldCreateSnapshot() {
		go s.createSnapshot()
	}
	
	return nil
}

// writeEvent 写入事件到ledger文件
func (s *Storage) writeEvent(event types.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	
	_, err = s.ledgerFile.Write(append(data, '\n'))
	if err != nil {
		return err
	}
	
	// 更新序列号
	if event.Sequence > s.lastSequence {
		s.lastSequence = event.Sequence
	}
	
	return s.ledgerFile.Sync()
}

// shouldCreateSnapshot 检查是否应该创建快照
func (s *Storage) shouldCreateSnapshot() bool {
	return s.eventCount >= s.maxEventsBeforeSnapshot ||
		time.Since(s.lastSnapshot) >= s.snapshotInterval
}

// createSnapshot 创建快照（原子写入：.tmp → rename）
func (s *Storage) createSnapshot() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	snapshot := map[string]interface{}{
		"timestamp":     time.Now().Unix(),
		"sequence":      s.lastSequence,
		"orders":        s.orderTable.GetAll(),
		"executions":    s.executionTable.GetAll(),
		"pairs":         s.pairTable.GetAll(),
		"event_count":   s.eventCount,
		"since_cursor":  s.sinceCursor,
		"cursors": map[string]interface{}{
			"last_trade_id":   0,
			"last_order_time": time.Now().Unix(),
		},
	}
	
	// 原子写入：先写到临时文件，再重命名
	snapshotPath := filepath.Join(s.dataDir, "snapshot.json")
	tempPath := snapshotPath + ".tmp"
	
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}
	
	// 写入临时文件
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp snapshot: %w", err)
	}
	
	if _, err := file.Write(data); err != nil {
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write temp snapshot: %w", err)
	}
	
	// 强制同步到磁盘
	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to sync temp snapshot: %w", err)
	}
	file.Close()
	
	// 原子重命名
	if err := os.Rename(tempPath, snapshotPath); err != nil {
		// 清理临时文件
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}
	
	// 同步目录
	if dir, err := os.Open(filepath.Dir(snapshotPath)); err == nil {
		dir.Sync()
		dir.Close()
	}
	
	s.lastSnapshot = time.Now()
	log.Printf("Created snapshot with sequence %d, %d events (atomic write)", s.lastSequence, s.eventCount)
	
	return nil
}

// loadSnapshot 加载快照
func (s *Storage) loadSnapshot() error {
	snapshotPath := filepath.Join(s.dataDir, "snapshot.json")
	
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // 快照文件不存在，这是正常的
		}
		return fmt.Errorf("failed to read snapshot: %w", err)
	}
	
	var snapshot map[string]interface{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}
	
	// 恢复订单记录
	if orders, ok := snapshot["orders"].(map[string]interface{}); ok {
		for _, orderData := range orders {
			orderBytes, _ := json.Marshal(orderData)
			var order types.Order
			if err := json.Unmarshal(orderBytes, &order); err == nil {
				s.orderTable.Add(&order)
			}
		}
	}
	
	// 恢复执行记录
	if executions, ok := snapshot["executions"].(map[string]interface{}); ok {
		for orderID, executionList := range executions {
			if execList, ok := executionList.([]interface{}); ok {
				for _, executionData := range execList {
					executionBytes, _ := json.Marshal(executionData)
					var execution types.Execution
					if err := json.Unmarshal(executionBytes, &execution); err == nil {
						s.executionTable.Add(orderID, &execution)
					}
				}
			}
		}
	}
	
	// 恢复配对记录
	if pairs, ok := snapshot["pairs"].(map[string]interface{}); ok {
		for _, pairData := range pairs {
			pairBytes, _ := json.Marshal(pairData)
			var pair types.OrderPair
			if err := json.Unmarshal(pairBytes, &pair); err == nil {
				s.pairTable.Add(&pair)
			}
		}
	}
	
	// 恢复事件计数
	if eventCount, ok := snapshot["event_count"].(float64); ok {
		s.eventCount = int(eventCount)
	}
	
	// 恢复序列号
	if sequence, ok := snapshot["sequence"].(float64); ok {
		s.lastSequence = int64(sequence)
	}
	
	// 恢复游标
	if sinceCursor, ok := snapshot["since_cursor"].(float64); ok {
		s.sinceCursor = int64(sinceCursor)
	}
	
	log.Printf("Loaded snapshot with %d orders, %d executions, %d pairs", 
		len(s.orderTable.GetAll()), 
		len(s.executionTable.GetAll()), 
		len(s.pairTable.GetAll()))
	
	return nil
}

// replayLedger 重放ledger
func (s *Storage) replayLedger() error {
	ledgerPath := filepath.Join(s.dataDir, "ledger.jsonl")
	
	file, err := os.Open(ledgerPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // ledger文件不存在，这是正常的
		}
		return fmt.Errorf("failed to open ledger: %w", err)
	}
	defer file.Close()
	
	log.Println("Starting ledger replay...")
	
	scanner := bufio.NewScanner(file)
	replayedCount := 0
	
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		var event types.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			log.Printf("Failed to parse ledger line: %v", err)
			continue
		}
		
		// 幂等回放事件到内存表
		if err := s.replayEvent(event); err != nil {
			log.Printf("Failed to replay event %d: %v", event.Sequence, err)
			continue
		}
		
		replayedCount++
		
		// 更新游标到最新处理的事件
		if event.Timestamp > s.sinceCursor {
			s.sinceCursor = event.Timestamp
		}
		
		// 更新序列号
		if event.Sequence > s.lastSequence {
			s.lastSequence = event.Sequence
		}
	}
	
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan ledger: %w", err)
	}
	
	log.Printf("Ledger replay completed: %d events replayed", replayedCount)
	return nil
}

// Close 关闭存储
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 创建最终快照
	if err := s.createSnapshot(); err != nil {
		log.Printf("Failed to create final snapshot: %v", err)
	}
	
	// 关闭ledger文件
	if s.ledgerFile != nil {
		return s.ledgerFile.Close()
	}
	
	return nil
}

// WriteRejection 写入拒绝记录
func (s *Storage) WriteRejection(rejection *types.Rejection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 创建拒绝文件路径
	rejectionPath := filepath.Join(s.dataDir, "rejections.jsonl")
	
	// 打开或创建拒绝文件
	file, err := os.OpenFile(rejectionPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open rejection file: %w", err)
	}
	defer file.Close()
	
	// 序列化拒绝记录
	data, err := json.Marshal(rejection)
	if err != nil {
		return fmt.Errorf("failed to marshal rejection: %w", err)
	}
	
	// 写入文件
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write rejection: %w", err)
	}
	
	// 强制同步到磁盘
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync rejection file: %w", err)
	}
	
	return nil
}

// WriteEvent 写入事件（公开方法）
func (s *Storage) WriteEvent(event types.Event) error {
	return s.writeEvent(event)
}

// LoadSnapshot 加载快照（公开方法）
func (s *Storage) LoadSnapshot() error {
	return s.loadSnapshot()
}

// GetOperationTable 获取操作表
func (s *Storage) GetOperationTable() *types.OperationTable {
	// 创建一个空的操作表，因为我们还没有实现操作表
	return types.NewOperationTable()
}

// LoadEventsAfter 加载指定时间后的事件（尾段回放）
func (s *Storage) LoadEventsAfter(timestamp int64) ([]types.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	ledgerPath := filepath.Join(s.dataDir, "ledger.jsonl")
	
	file, err := os.Open(ledgerPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []types.Event{}, nil // ledger文件不存在是正常的
		}
		return nil, fmt.Errorf("failed to open ledger: %w", err)
	}
	defer file.Close()
	
	var events []types.Event
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		var event types.Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			log.Printf("Failed to parse event line: %v", err)
			continue
		}
		
		// 只加载指定时间戳之后的事件
		if event.Timestamp > timestamp {
			events = append(events, event)
		}
	}
	
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan ledger: %w", err)
	}
	
	log.Printf("Loaded %d events after timestamp %d", len(events), timestamp)
	return events, nil
}

// UpdateSinceCursor 更新游标位置
func (s *Storage) UpdateSinceCursor(cursor int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.sinceCursor = cursor
	log.Printf("Updated since cursor to: %d", cursor)
}

// GetSinceCursor 获取当前游标位置
func (s *Storage) GetSinceCursor() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return s.sinceCursor
}

// UpdateTradeCursor 更新交易游标位置
func (s *Storage) UpdateTradeCursor(cursor int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	s.sinceCursor = cursor
	
	// 持久化游标到文件
	cursorPath := filepath.Join(s.dataDir, "trade_cursor.txt")
	if err := os.WriteFile(cursorPath, []byte(fmt.Sprintf("%d", cursor)), 0644); err != nil {
		log.Printf("Failed to persist trade cursor: %v", err)
		return err
	}
	
	log.Printf("Updated trade cursor to: %d", cursor)
	return nil
}

// GetTradeCursor 获取交易游标位置
func (s *Storage) GetTradeCursor() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return s.sinceCursor
}

// LoadTradeCursor 从文件加载交易游标
func (s *Storage) LoadTradeCursor() int64 {
	cursorPath := filepath.Join(s.dataDir, "trade_cursor.txt")
	data, err := os.ReadFile(cursorPath)
	if err != nil {
		log.Printf("Failed to load trade cursor: %v", err)
		return 0
	}
	
	var cursor int64
	if _, err := fmt.Sscanf(string(data), "%d", &cursor); err != nil {
		log.Printf("Failed to parse trade cursor: %v", err)
		return 0
	}
	
	s.mu.Lock()
	s.sinceCursor = cursor
	s.mu.Unlock()
	
	log.Printf("Loaded trade cursor: %d", cursor)
	return cursor
}

// LightReconciliation 轻对账：从游标位置开始回放事件
func (s *Storage) LightReconciliation() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// 从游标位置加载事件
	events, err := s.LoadEventsAfter(s.sinceCursor)
	if err != nil {
		return fmt.Errorf("failed to load events for reconciliation: %w", err)
	}
	
	if len(events) == 0 {
		log.Println("No events to reconcile")
		return nil
	}
	
	// 回放事件到内存表
	reconciledCount := 0
	for _, event := range events {
		if err := s.replayEvent(event); err != nil {
			log.Printf("Failed to replay event %d: %v", event.Sequence, err)
			continue
		}
		reconciledCount++
		
		// 更新游标到最新处理的事件
		if event.Timestamp > s.sinceCursor {
			s.sinceCursor = event.Timestamp
		}
	}
	
	log.Printf("Light reconciliation completed: %d events replayed", reconciledCount)
	return nil
}

// replayEvent 回放单个事件到内存表
func (s *Storage) replayEvent(event types.Event) error {
	switch event.Type {
	case types.EventOrderPlaced:
		return s.replayOrderPlaced(event)
	case types.EventOrderFilled:
		return s.replayOrderFilled(event)
	case types.EventOrderCanceled:
		return s.replayOrderCanceled(event)
	case types.EventGridPaired:
		return s.replayGridPaired(event)
	default:
		// 其他事件类型暂时跳过
		return nil
	}
}

// replayOrderPlaced 回放订单下单事件
func (s *Storage) replayOrderPlaced(event types.Event) error {
	// 检查订单是否已存在（幂等性）
	if _, exists := s.orderTable.Get(event.OrderID); exists {
		return nil // 订单已存在，跳过
	}
	
	// 创建订单
	order := &types.Order{
		ClientOrderID: event.OrderID,
		Symbol:        extractSymbolFromEvent(event),
		Status:        types.OrderStatusNew,
		Side:          event.Side,
		Price:         event.Price,
		OrigQty:       event.Quantity,
		ExecutedQty:   0,
		ReduceOnly:    event.ReduceOnly,
		CreatedAt:     time.Unix(0, event.Timestamp),
		UpdatedAt:     time.Unix(0, event.Timestamp),
	}
	
	s.orderTable.Add(order)
	return nil
}

// replayOrderFilled 回放订单成交事件
func (s *Storage) replayOrderFilled(event types.Event) error {
	// 更新订单状态
	if order, exists := s.orderTable.Get(event.OrderID); exists {
		// 计算新的已成交数量（取最大值，确保只前进）
		newFilledQty := maxFloat64(order.ExecutedQty, event.FilledQty)
		if newFilledQty > order.ExecutedQty {
			order.ExecutedQty = newFilledQty
			order.UpdatedAt = time.Unix(0, event.Timestamp)
			
			// 更新订单状态
			if order.ExecutedQty >= order.OrigQty {
				order.Status = types.OrderStatusFilled
			} else {
				order.Status = types.OrderStatusPartiallyFilled
			}
			
			s.orderTable.Update(order)
		}
	}
	
	// 记录成交明细
	execution := &types.Execution{
		OrderID: event.OrderID,
		TradeID: fmt.Sprintf("%d", event.Timestamp),
		Price:   event.AvgPrice,
		Qty:     event.Quantity,
		Time:    time.Unix(0, event.Timestamp),
	}
	
	s.executionTable.Add(event.OrderID, execution)
	return nil
}

// replayOrderCanceled 回放订单取消事件
func (s *Storage) replayOrderCanceled(event types.Event) error {
	// 更新订单状态
	if order, exists := s.orderTable.Get(event.OrderID); exists {
		if order.Status.IsActive() {
			order.Status = types.OrderStatusCanceled
			order.UpdatedAt = time.Unix(0, event.Timestamp)
			s.orderTable.Update(order)
		}
	}
	
	return nil
}

// replayGridPaired 回放网格配对事件
func (s *Storage) replayGridPaired(event types.Event) error {
	// 检查配对是否已存在（幂等性）
	if _, exists := s.pairTable.Get(event.GridID); exists {
		return nil // 配对已存在，跳过
	}
	
	// 创建配对记录
	pair := &types.OrderPair{
		GridLegID:   event.GridID,
		Price:       event.Price,
		Qty:         event.Quantity,
		MatchedAt:   time.Unix(0, event.Timestamp),
	}
	
	s.pairTable.Add(pair)
	return nil
}

// 辅助函数
func extractSymbolFromEvent(event types.Event) string {
	if event.Extra != nil {
		if symbol, ok := event.Extra["symbol"].(string); ok {
			return symbol
		}
	}
	return ""
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// LoadDeferredOrders 加载延迟订单（FIFO顺序）
func (s *Storage) LoadDeferredOrders() ([]*types.DeferredOrder, error) {
	deferredPath := filepath.Join(s.dataDir, "deferred_queue.jsonl")
	
	file, err := os.Open(deferredPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*types.DeferredOrder{}, nil // 文件不存在是正常的
		}
		return nil, fmt.Errorf("failed to open deferred queue: %w", err)
	}
	defer file.Close()
	
	var orders []*types.DeferredOrder
	scanner := bufio.NewScanner(file)
	
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		var order types.DeferredOrder
		if err := json.Unmarshal([]byte(line), &order); err != nil {
			log.Printf("Failed to parse deferred order line: %v", err)
			continue
		}
		
		orders = append(orders, &order)
	}
	
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan deferred queue: %w", err)
	}
	
	log.Printf("Loaded %d deferred orders", len(orders))
	return orders, nil
}

// WriteDeferredOrder 写入延迟订单（追加到队尾）
func (s *Storage) WriteDeferredOrder(order *types.DeferredOrder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	deferredPath := filepath.Join(s.dataDir, "deferred_queue.jsonl")
	
	// 打开或创建文件（追加模式）
	file, err := os.OpenFile(deferredPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open deferred queue: %w", err)
	}
	defer file.Close()
	
	// 序列化订单
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal deferred order: %w", err)
	}
	
	// 写入文件
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write deferred order: %w", err)
	}
	
	// 强制同步到磁盘（关键事件）
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync deferred queue: %w", err)
	}
	
	log.Printf("Deferred order written: %s %s %.8f@%.8f", 
		order.GridID, order.Side, order.Quantity, order.Price)
	
	return nil
}

// PopDeferredOrder 从队头弹出延迟订单（FIFO）
func (s *Storage) PopDeferredOrder() (*types.DeferredOrder, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	deferredPath := filepath.Join(s.dataDir, "deferred_queue.jsonl")
	tempPath := deferredPath + ".tmp"
	
	// 读取所有订单
	orders, err := s.LoadDeferredOrders()
	if err != nil {
		return nil, fmt.Errorf("failed to load deferred orders: %w", err)
	}
	
	if len(orders) == 0 {
		return nil, nil // 队列为空
	}
	
	// 取队头订单
	firstOrder := orders[0]
	remainingOrders := orders[1:]
	
	// 原子重写文件：先写临时文件，再重命名
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp deferred queue: %w", err)
	}
	defer tempFile.Close()
	
	// 写入剩余订单
	for _, order := range remainingOrders {
		data, err := json.Marshal(order)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal remaining order: %w", err)
		}
		
		if _, err := tempFile.Write(append(data, '\n')); err != nil {
			return nil, fmt.Errorf("failed to write remaining order: %w", err)
		}
	}
	
	// 同步临时文件
	if err := tempFile.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync temp deferred queue: %w", err)
	}
	tempFile.Close()
	
	// 原子重命名
	if err := os.Rename(tempPath, deferredPath); err != nil {
		os.Remove(tempPath) // 清理临时文件
		return nil, fmt.Errorf("failed to rename deferred queue: %w", err)
	}
	
	log.Printf("Deferred order popped: %s %s %.8f@%.8f (remaining: %d)", 
		firstOrder.GridID, firstOrder.Side, firstOrder.Quantity, firstOrder.Price, len(remainingOrders))
	
	return firstOrder, nil
}

// ClearDeferredOrders 清除延迟订单队列
func (s *Storage) ClearDeferredOrders() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	deferredPath := filepath.Join(s.dataDir, "deferred_queue.jsonl")
	
	// 删除文件
	if err := os.Remove(deferredPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clear deferred queue: %w", err)
	}
	
	log.Println("Deferred queue cleared")
	return nil
}
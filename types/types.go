package types

import (
	"fmt"
	"sync"
	"time"
)

// OrderStatus 订单状态枚举
type OrderStatus string

const (
	OrderStatusNew             OrderStatus = "NEW"
	OrderStatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	OrderStatusFilled          OrderStatus = "FILLED"
	OrderStatusCanceled        OrderStatus = "CANCELED"
	OrderStatusExpired         OrderStatus = "EXPIRED"
)

// OrderSide 订单方向
type OrderSide string

const (
	OrderSideBuy  OrderSide = "BUY"
	OrderSideSell OrderSide = "SELL"
)

// PositionSide 持仓方向
type PositionSide string

const (
	PositionSideLong  PositionSide = "LONG"
	PositionSideShort PositionSide = "SHORT"
	PositionSideBoth  PositionSide = "BOTH"
)

// OrderType 订单类型
type OrderType string

const (
	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeMarket OrderType = "MARKET"
)

// TimeInForce 有效期类型
type TimeInForce string

const (
	TimeInForceGTC TimeInForce = "GTC" // Good Till Cancel
	TimeInForceIOC TimeInForce = "IOC" // Immediate Or Cancel
	TimeInForceFOK TimeInForce = "FOK" // Fill Or Kill
)

// Order 订单结构
type Order struct {
	ClientOrderID string      `json:"clientOrderId"`
	OrderID       string      `json:"orderId"`
	Symbol        string      `json:"symbol"`
	Status        OrderStatus `json:"status"`
	OrderType     OrderType   `json:"orderType"`
	Side          OrderSide   `json:"side"`
	PositionSide  PositionSide `json:"positionSide"`
	Price         float64     `json:"price"`
	OrigQty       float64     `json:"origQty"`
	ExecutedQty   float64     `json:"executedQty"`
	ReduceOnly    bool        `json:"reduceOnly"`
	TimeInForce   TimeInForce `json:"timeInForce"`
	CreatedAt     time.Time   `json:"createdAt"`
	UpdatedAt     time.Time   `json:"updatedAt"`
	GridLegID     string      `json:"gridLegId,omitempty"` // 网格腿ID，用于配对
}

// Execution 成交明细
type Execution struct {
	OrderID string    `json:"orderId"`
	TradeID string    `json:"tradeId"`
	Price   float64   `json:"price"`
	Qty     float64   `json:"qty"`
	Time    time.Time `json:"time"`
}

// Operation 操作记录
type Operation struct {
	ClientOrderID string                 `json:"clientOrderId"`
	Operation     string                 `json:"operation"`
	Status        string                 `json:"status"`
	Timestamp     time.Time              `json:"timestamp"`
	RetryCount    int                    `json:"retryCount"`
	Response      map[string]interface{} `json:"response,omitempty"`
}

// OrderPair 订单配对
type OrderPair struct {
	GridLegID    string    `json:"gridLegId"`
	BuyOrderID   string    `json:"buyOrderId"`
	SellOrderID  string    `json:"sellOrderId"`
	BuyTradeID   string    `json:"buyTradeId"`
	SellTradeID  string    `json:"sellTradeId"`
	Price        float64   `json:"price"`
	Qty          float64   `json:"qty"`
	MatchedAt    time.Time `json:"matchedAt"`
	ProfitLoss   float64   `json:"profitLoss"`
}

// GridLevel 网格级别
type GridLevel struct {
	Price       float64 `json:"price"`
	BuyOrderID  string  `json:"buyOrderId,omitempty"`
	SellOrderID string  `json:"sellOrderId,omitempty"`
	HasPosition bool    `json:"hasPosition"` // 是否持有该格仓位
	GridLegID   string  `json:"gridLegId,omitempty"`
}

// GridConfig 网格配置
type GridConfig struct {
	Symbol        string  `json:"symbol"`
	PriceMin      float64 `json:"priceMin"`
	PriceMax      float64 `json:"priceMax"`
	StepSize      float64 `json:"stepSize"`
	OrderQty      float64 `json:"orderQty"`
	MaxBuyOrders  int     `json:"maxBuyOrders"`
	MaxSellOrders int     `json:"maxSellOrders"`
	Levels        []GridLevel `json:"levels"`
}

// EventType 事件类型
type EventType string

const (
	// 写屏障事件 (关键事件，需要同步写入)
	EventOrderIntent   EventType = "ORDER_INTENT"
	
	// 订单生命周期事件 (关键事件，需要同步写入)
	EventOrderRejected EventType = "ORDER_REJECTED"
	EventOrderPlaced   EventType = "ORDER_PLACED"
	EventOrderFilled   EventType = "ORDER_FILLED"
	EventOrderCanceled EventType = "ORDER_CANCELED"
	
	// 网格配对事件
	EventGridPaired    EventType = "GRID_PAIRED"
	EventBuyToSell     EventType = "BUY_TO_SELL"
	EventSellToBuy     EventType = "SELL_TO_BUY"
	EventManualAction  EventType = "MANUAL_ACTION"
	
	// 候补队列相关事件 (关键事件，需要同步写入)
	EventDeferredEnqueue EventType = "DEFERRED_ENQUEUE"
	EventDeferredDequeue EventType = "DEFERRED_DEQUEUE"
	EventDeferredRetry   EventType = "DEFERRED_RETRY"
	
	// 手动操作相关事件 (关键事件，需要同步写入)
	EventManualCancel    EventType = "MANUAL_CANCEL"
	EventManualModify    EventType = "MANUAL_MODIFY"
	EventManualStop      EventType = "MANUAL_STOP"
	EventManualStart     EventType = "MANUAL_START"
	
	// 交易执行和持仓更新事件
	EventTypeExecution     EventType = "EXECUTION"
	EventTypePositionUpdate EventType = "POSITION_UPDATE"
	
	// 系统状态变更事件
	EventTypeSystemStateChanged EventType = "SYSTEM_STATE_CHANGED"
)

// Event 事件记录
type Event struct {
	Timestamp   int64                  `json:"ts"`
	Sequence    int64                  `json:"seq"`
	Type        EventType              `json:"type"`
	OrderID     string                 `json:"oid,omitempty"`
	GridID      string                 `json:"grid,omitempty"`
	Side        OrderSide              `json:"side,omitempty"`
	Price       float64                `json:"px,omitempty"`
	Quantity    float64                `json:"qty,omitempty"`
	FilledQty   float64                `json:"filled,omitempty"`
	AvgPrice    float64                `json:"avg_px,omitempty"`
	ReduceOnly  bool                   `json:"reduceOnly,omitempty"`
	Source      string                 `json:"source,omitempty"` // WS/REST
	LatencyMs   int64                  `json:"latency_ms,omitempty"`
	Data        []byte                 `json:"data,omitempty"`   // 事件数据
	Extra       map[string]interface{} `json:"extra,omitempty"`
}

// Snapshot 系统快照
type Snapshot struct {
	Timestamp   time.Time                         `json:"timestamp"`
	Sequence    int64                             `json:"sequence"`
	OrderIndex  map[string]*Order                 `json:"orderIndex"`
	GridIndex   map[string]*GridConfig            `json:"gridIndex"`
	Cursors     map[string]interface{}            `json:"cursors"`
	ActivePairs map[string]*OrderPair             `json:"activePairs"`
	Orders      map[string]Order                  `json:"orders"`
	Executions  map[string][]Execution            `json:"executions"`
	Operations  []Operation                       `json:"operations"`
	Pairs       map[string]OrderPair              `json:"pairs"`
}

// DeferredOrder 候补订单
type DeferredOrder struct {
	GridID        string    `json:"grid"`
	ClientOrderID string    `json:"clientOrderId"`
	Symbol        string    `json:"symbol"`
	Side          OrderSide `json:"side"`
	Price         float64   `json:"px"`
	Quantity      float64   `json:"qty"`
	Reason        string    `json:"reason"`
	Time          time.Time `json:"time"`
}

// Rejection 拒绝记录
type Rejection struct {
	Timestamp   int64                  `json:"ts"`
	OrderID     string                 `json:"oid"`
	Symbol      string                 `json:"symbol"`
	Side        OrderSide              `json:"side"`
	Price       float64                `json:"px"`
	Quantity    float64                `json:"qty"`
	Code        int                    `json:"code"`
	Message     string                 `json:"msg"`
	RawResponse map[string]interface{} `json:"raw_response,omitempty"`
}

// SystemState 系统状态
type SystemState string

const (
	SystemStateNormal    SystemState = "NORMAL"
	SystemStateDegraded  SystemState = "DEGRADED"
	SystemStateRecovering SystemState = "RECOVERING"
)

// MarketData 市场数据
type MarketData struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

// AccountInfo 账户信息
type AccountInfo struct {
	TotalWalletBalance          string `json:"totalWalletBalance"`
	TotalUnrealizedPnl          string `json:"totalUnrealizedPnl"`
	TotalMarginBalance          string `json:"totalMarginBalance"`
	TotalPositionInitialMargin  string `json:"totalPositionInitialMargin"`
	TotalOpenOrderInitialMargin string `json:"totalOpenOrderInitialMargin"`
	TotalCrossWalletBalance     string `json:"totalCrossWalletBalance"`
	TotalCrossUnPnl             string `json:"totalCrossUnPnl"`
	AvailableBalance            string `json:"availableBalance"`
	MaxWithdrawAmount           string `json:"maxWithdrawAmount"`
	Balances                    []Balance `json:"balances"`
}

type Balance struct {
	Asset               string `json:"asset"`
	WalletBalance       string `json:"walletBalance"`
	UnrealizedProfit    string `json:"unrealizedProfit"`
	MarginBalance       string `json:"marginBalance"`
	MaintMargin         string `json:"maintMargin"`
	InitialMargin       string `json:"initialMargin"`
	PositionInitialMargin string `json:"positionInitialMargin"`
	OpenOrderInitialMargin string `json:"openOrderInitialMargin"`
	CrossWalletBalance  string `json:"crossWalletBalance"`
	CrossUnPnl          string `json:"crossUnPnl"`
	AvailableBalance    string `json:"availableBalance"`
	MaxWithdrawAmount   string `json:"maxWithdrawAmount"`
}

// Position 持仓信息
type Position struct {
	Symbol                 string  `json:"symbol"`
	PositionAmt           string  `json:"positionAmt"`
	EntryPrice            string  `json:"entryPrice"`
	MarkPrice             string  `json:"markPrice"`
	UnRealizedProfit      string  `json:"unRealizedProfit"`
	LiquidationPrice      string  `json:"liquidationPrice"`
	Leverage              string  `json:"leverage"`
	MaxNotionalValue      string  `json:"maxNotionalValue"`
	MarginType            string  `json:"marginType"`
	IsolatedMargin        string  `json:"isolatedMargin"`
	IsAutoAddMargin       string  `json:"isAutoAddMargin"`
	PositionSide          string  `json:"positionSide"`
	Notional              string  `json:"notional"`
	IsolatedWallet        string  `json:"isolatedWallet"`
	UpdateTime            int64   `json:"updateTime"`
}

// IsTerminal 判断订单状态是否为终结状态
func (s OrderStatus) IsTerminal() bool {
	return s == OrderStatusFilled || s == OrderStatusCanceled || s == OrderStatusExpired
}

// IsActive 判断订单状态是否为活跃状态
func (s OrderStatus) IsActive() bool {
	return s == OrderStatusNew || s == OrderStatusPartiallyFilled
}

// OrderTable 订单表 - 内存中的订单索引
type OrderTable struct {
	Orders map[string]*Order `json:"orders"` // key: clientOrderId
	mu     sync.RWMutex      `json:"-"`
}

// NewOrderTable 创建新的订单表
func NewOrderTable() *OrderTable {
	return &OrderTable{
		Orders: make(map[string]*Order),
	}
}

// Add 添加订单
func (ot *OrderTable) Add(order *Order) {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	ot.Orders[order.ClientOrderID] = order
}

// Get 获取订单
func (ot *OrderTable) Get(clientOrderID string) (*Order, bool) {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	order, exists := ot.Orders[clientOrderID]
	return order, exists
}

// Update 更新订单
func (ot *OrderTable) Update(order *Order) {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	ot.Orders[order.ClientOrderID] = order
}

// Remove 移除订单
func (ot *OrderTable) Remove(clientOrderID string) {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	delete(ot.Orders, clientOrderID)
}

// GetAll 获取所有订单
func (ot *OrderTable) GetAll() map[string]*Order {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	
	result := make(map[string]*Order)
	for k, v := range ot.Orders {
		result[k] = v
	}
	return result
}

// GetActiveOrders 获取活跃订单
func (ot *OrderTable) GetActiveOrders() []*Order {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	
	var activeOrders []*Order
	for _, order := range ot.Orders {
		if order.Status.IsActive() {
			activeOrders = append(activeOrders, order)
		}
	}
	return activeOrders
}

// GetActiveCount 获取活跃订单数量
func (ot *OrderTable) GetActiveCount() int {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	
	count := 0
	for _, order := range ot.Orders {
		if order.Status.IsActive() {
			count++
		}
	}
	return count
}

// ExecutionTable 成交明细表
type ExecutionTable struct {
	Executions map[string][]*Execution `json:"executions"` // key: clientOrderId
	mu         sync.RWMutex            `json:"-"`
}

// NewExecutionTable 创建新的成交明细表
func NewExecutionTable() *ExecutionTable {
	return &ExecutionTable{
		Executions: make(map[string][]*Execution),
	}
}

// Add 添加成交记录
func (et *ExecutionTable) Add(clientOrderID string, execution *Execution) {
	et.mu.Lock()
	defer et.mu.Unlock()
	et.Executions[clientOrderID] = append(et.Executions[clientOrderID], execution)
}

// Get 获取订单的所有成交记录
func (et *ExecutionTable) Get(clientOrderID string) []*Execution {
	et.mu.RLock()
	defer et.mu.RUnlock()
	return et.Executions[clientOrderID]
}

// GetAll 获取所有成交记录
func (et *ExecutionTable) GetAll() map[string][]*Execution {
	et.mu.RLock()
	defer et.mu.RUnlock()
	
	result := make(map[string][]*Execution)
	for k, v := range et.Executions {
		result[k] = v
	}
	return result
}

// OperationTable 操作日志表
type OperationTable struct {
	Operations []Operation  `json:"operations"`
	mu         sync.RWMutex `json:"-"`
}

// NewOperationTable 创建新的操作日志表
func NewOperationTable() *OperationTable {
	return &OperationTable{
		Operations: make([]Operation, 0),
	}
}

// Add 添加操作记录
func (ot *OperationTable) Add(operation Operation) {
	ot.mu.Lock()
	defer ot.mu.Unlock()
	ot.Operations = append(ot.Operations, operation)
}

// GetRecent 获取最近的操作记录
func (ot *OperationTable) GetRecent(limit int) []Operation {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	
	if len(ot.Operations) <= limit {
		return ot.Operations
	}
	
	return ot.Operations[len(ot.Operations)-limit:]
}

// GetAll 获取所有操作记录
func (ot *OperationTable) GetAll() []Operation {
	ot.mu.RLock()
	defer ot.mu.RUnlock()
	
	result := make([]Operation, len(ot.Operations))
	copy(result, ot.Operations)
	return result
}

// PairTable 配对表
type PairTable struct {
	Pairs map[string]*OrderPair `json:"pairs"` // key: gridLegId
	mu    sync.RWMutex          `json:"-"`
}

// NewPairTable 创建新的配对表
func NewPairTable() *PairTable {
	return &PairTable{
		Pairs: make(map[string]*OrderPair),
	}
}

// Add 添加配对
func (pt *PairTable) Add(pair *OrderPair) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.Pairs[pair.GridLegID] = pair
}

// Get 获取配对
func (pt *PairTable) Get(gridLegID string) (*OrderPair, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	pair, exists := pt.Pairs[gridLegID]
	return pair, exists
}

// Update 更新配对
func (pt *PairTable) Update(pair *OrderPair) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.Pairs[pair.GridLegID] = pair
}

// Remove 移除配对
func (pt *PairTable) Remove(gridLegID string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	delete(pt.Pairs, gridLegID)
}

// GetAll 获取所有配对
func (pt *PairTable) GetAll() map[string]*OrderPair {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	
	result := make(map[string]*OrderPair)
	for k, v := range pt.Pairs {
		result[k] = v
	}
	return result
}

// GetActivePairs 获取活跃配对（未完成的配对）
func (pt *PairTable) GetActivePairs() []*OrderPair {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	
	var activePairs []*OrderPair
	for _, pair := range pt.Pairs {
		// 如果买单或卖单还没有成交ID，说明配对还未完成
		if pair.BuyTradeID == "" || pair.SellTradeID == "" {
			activePairs = append(activePairs, pair)
		}
	}
	return activePairs
}

// GridLegIDGenerator 网格腿ID生成器
type GridLegIDGenerator struct {
	counter int64
	mu      sync.Mutex
}

// NewGridLegIDGenerator 创建新的网格腿ID生成器
func NewGridLegIDGenerator() *GridLegIDGenerator {
	return &GridLegIDGenerator{
		counter: 0,
	}
}

// Generate 生成新的网格腿ID
func (g *GridLegIDGenerator) Generate(symbol string, price float64) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	g.counter++
	return fmt.Sprintf("%s_%.2f_%d", symbol, price, g.counter)
}

// MultiGridConfig 多网格配置
type MultiGridConfig struct {
	GridID        string      `json:"gridId"`
	Symbol        string      `json:"symbol"`
	GridType      string      `json:"gridType"`
	GridLevels    int         `json:"gridLevels"`
	LowerPrice    float64     `json:"lowerPrice"`
	UpperPrice    float64     `json:"upperPrice"`
	GridSpacing   float64     `json:"gridSpacing"`
	OrderQuantity float64     `json:"orderQuantity"`
	XSegment      *GridConfig `json:"x_segment,omitempty"` // X段网格
	ASegment      *GridConfig `json:"a_segment,omitempty"` // A段网格  
	BSegment      *GridConfig `json:"b_segment,omitempty"` // B段网格
}

// ErrorCode 错误代码
type ErrorCode string

const (
	ErrorCodeRateLimit        ErrorCode = "RATE_LIMIT"
	ErrorCodeInsufficientBalance ErrorCode = "INSUFFICIENT_BALANCE"
	ErrorCodePriceOutOfRange  ErrorCode = "PRICE_OUT_OF_RANGE"
	ErrorCodeOrderExists      ErrorCode = "ORDER_EXISTS"
	ErrorCodeNetworkError     ErrorCode = "NETWORK_ERROR"
	ErrorCodeOrderNotFound    ErrorCode = "ORDER_NOT_FOUND"
	ErrorCodeMarketDataError  ErrorCode = "MARKET_DATA_ERROR"
	ErrorCodePermissionDenied ErrorCode = "PERMISSION_DENIED"
	ErrorCodeUnknown          ErrorCode = "UNKNOWN"
	ErrorCodeRequestBuildError ErrorCode = "REQUEST_BUILD_ERROR"
	ErrorCodeResponseReadError ErrorCode = "RESPONSE_READ_ERROR"
	ErrorCodeJSONParseError   ErrorCode = "JSON_PARSE_ERROR"
	ErrorCodeNone             ErrorCode = ""
)

// TradingError 交易错误
type TradingError struct {
	Code       ErrorCode              `json:"code"`
	Message    string                 `json:"message"`
	Timestamp  time.Time              `json:"timestamp"`
	Context    map[string]interface{} `json:"context,omitempty"`
	Retryable  bool                   `json:"retryable"`
	RetryAfter time.Duration          `json:"retryAfter,omitempty"`
}

// Error 实现error接口
func (e *TradingError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// NewTradingError 创建新的交易错误
func NewTradingError(code ErrorCode, message string, context map[string]interface{}) *TradingError {
	return &TradingError{
		Code:      code,
		Message:   message,
		Timestamp: time.Now(),
		Context:   context,
		Retryable: false,
	}
}

// ExchangeInfo 交易所信息
type ExchangeInfo struct {
	Timezone   string       `json:"timezone"`
	ServerTime int64        `json:"serverTime"`
	Symbols    []SymbolInfo `json:"symbols"`
}

// SymbolInfo 交易对信息
type SymbolInfo struct {
	Symbol                string        `json:"symbol"`
	Status                string        `json:"status"`
	BaseAsset             string        `json:"baseAsset"`
	BaseAssetPrecision    int           `json:"baseAssetPrecision"`
	QuoteAsset            string        `json:"quoteAsset"`
	QuoteAssetPrecision   int           `json:"quoteAssetPrecision"`
	OrderTypes            []string      `json:"orderTypes"`
	IcebergAllowed        bool          `json:"icebergAllowed"`
	OcoAllowed            bool          `json:"ocoAllowed"`
	IsSpotTradingAllowed  bool          `json:"isSpotTradingAllowed"`
	IsMarginTradingAllowed bool         `json:"isMarginTradingAllowed"`
	Filters               []SymbolFilter `json:"filters"`
}

// SymbolFilter 交易对过滤器
type SymbolFilter struct {
	FilterType       string `json:"filterType"`
	MinPrice         string `json:"minPrice,omitempty"`
	MaxPrice         string `json:"maxPrice,omitempty"`
	TickSize         string `json:"tickSize,omitempty"`
	MinQty           string `json:"minQty,omitempty"`
	MaxQty           string `json:"maxQty,omitempty"`
	StepSize         string `json:"stepSize,omitempty"`
	MinNotional      string `json:"minNotional,omitempty"`
	ApplyToMarket    bool   `json:"applyToMarket,omitempty"`
	AvgPriceMins     int    `json:"avgPriceMins,omitempty"`
	MultiplierUp     string `json:"multiplierUp,omitempty"`
	MultiplierDown   string `json:"multiplierDown,omitempty"`
	MultiplierDecimal string `json:"multiplierDecimal,omitempty"`
}

// TradingRules 交易规则缓存
type TradingRules struct {
	Symbol       string  `json:"symbol"`
	TickSize     float64 `json:"tickSize"`
	StepSize     float64 `json:"stepSize"`
	MinQty       float64 `json:"minQty"`
	MinNotional  float64 `json:"minNotional"`
	MaxQty       float64 `json:"maxQty"`
	MaxPrice     float64 `json:"maxPrice"`
	MinPrice     float64 `json:"minPrice"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// ValidationResult 验证结果
type ValidationResult struct {
	Valid            bool    `json:"valid"`
	AdjustedPrice    float64 `json:"adjustedPrice,omitempty"`
	AdjustedQuantity float64 `json:"adjustedQuantity,omitempty"`
	Reason           string  `json:"reason,omitempty"`
}
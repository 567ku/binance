package binance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"math"
	"sync"

	"binance-grid-trader/config"
	"binance-grid-trader/types"
	"binance-grid-trader/reducer"
)

// Client 币安API客户端
type Client struct {
	baseURL    string
	apiKey     string
	secretKey  string
	recvWindow int
	httpClient *http.Client
	
	// 交易规则缓存
	tradingRules map[string]*types.TradingRules
	rulesMutex   sync.RWMutex
	
	// Reducer 统一事件处理器
	reducer *reducer.Reducer
}

// NewClient 创建新的币安客户端
func NewClient(cfg *config.Config, creds *config.APICredentials) *Client {
	return &Client{
		baseURL:      cfg.API.BaseURL,
		apiKey:       creds.APIKey,
		secretKey:    creds.SecretKey,
		recvWindow:   cfg.System.RecvWindow,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		tradingRules: make(map[string]*types.TradingRules),
	}
}

// PlaceOrderRequest 下单请求参数
type PlaceOrderRequest struct {
	Symbol           string  `json:"symbol"`
	Side             string  `json:"side"`
	PositionSide     string  `json:"positionSide"`
	Type             string  `json:"type"`
	TimeInForce      string  `json:"timeInForce,omitempty"`
	Quantity         float64 `json:"quantity"`
	Price            float64 `json:"price,omitempty"`
	NewClientOrderID string  `json:"newClientOrderId,omitempty"`
	ReduceOnly       bool    `json:"reduceOnly,omitempty"`
}

// PlaceOrderResponse 下单响应
type PlaceOrderResponse struct {
	OrderID       int64   `json:"orderId"`
	Symbol        string  `json:"symbol"`
	Status        string  `json:"status"`
	ClientOrderID string  `json:"clientOrderId"`
	Price         string  `json:"price"`
	AvgPrice      string  `json:"avgPrice"`
	OrigQty       string  `json:"origQty"`
	ExecutedQty   string  `json:"executedQty"`
	CumQty        string  `json:"cumQty"`
	CumQuote      string  `json:"cumQuote"`
	TimeInForce   string  `json:"timeInForce"`
	Type          string  `json:"type"`
	ReduceOnly    bool    `json:"reduceOnly"`
	ClosePosition bool    `json:"closePosition"`
	Side          string  `json:"side"`
	PositionSide  string  `json:"positionSide"`
	StopPrice     string  `json:"stopPrice"`
	WorkingType   string  `json:"workingType"`
	PriceProtect  bool    `json:"priceProtect"`
	OrigType      string  `json:"origType"`
	UpdateTime    int64   `json:"updateTime"`
}

// CancelOrderRequest 撤单请求参数
type CancelOrderRequest struct {
	Symbol            string `json:"symbol"`
	OrderID           int64  `json:"orderId,omitempty"`
	OrigClientOrderID string `json:"origClientOrderId,omitempty"`
}

// QueryOrderRequest 查询订单请求参数
type QueryOrderRequest struct {
	Symbol            string `json:"symbol"`
	OrderID           int64  `json:"orderId,omitempty"`
	OrigClientOrderID string `json:"origClientOrderId,omitempty"`
}

// SetReducer 设置Reducer
func (c *Client) SetReducer(r *reducer.Reducer) {
	c.reducer = r
}

// PlaceOrder 下单
func (c *Client) PlaceOrder(req *PlaceOrderRequest) (*PlaceOrderResponse, error) {
	// 先进行精度和阈值验证
	side := types.OrderSideBuy
	if req.Side == "SELL" {
		side = types.OrderSideSell
	}
	
	validation := c.ValidateOrder(req.Symbol, side, req.Price, req.Quantity)
	if !validation.Valid {
		// 记录订单拒绝事件
		if c.reducer != nil {
			if err := c.reducer.WriteOrderRejected(req.NewClientOrderID, req.Symbol, side, req.Price, req.Quantity, validation.Reason); err != nil {
				log.Printf("Failed to write order rejected event: %v", err)
			}
		}
		return nil, fmt.Errorf("order validation failed: %s", validation.Reason)
	}
	
	// 使用验证后的价格和数量
	adjustedPrice := validation.AdjustedPrice
	adjustedQty := validation.AdjustedQuantity
	
	// 卖单参数下沉：强制设置reduceOnly=true
	if req.Side == "SELL" {
		req.ReduceOnly = true
	}
	
	// 写屏障：先写入ORDER_INTENT事件
	if c.reducer != nil {
		if err := c.reducer.WriteOrderIntent(req.NewClientOrderID, req.Symbol, side, adjustedPrice, adjustedQty); err != nil {
			return nil, fmt.Errorf("failed to write order intent: %w", err)
		}
	}
	
	params := make(url.Values)
	params.Set("symbol", req.Symbol)
	params.Set("side", req.Side)
	params.Set("type", req.Type)
	params.Set("quantity", fmt.Sprintf("%.8f", adjustedQty))

	if req.PositionSide != "" {
		params.Set("positionSide", req.PositionSide)
	}
	if adjustedPrice > 0 {
		params.Set("price", fmt.Sprintf("%.8f", adjustedPrice))
	}
	if req.TimeInForce != "" {
		params.Set("timeInForce", req.TimeInForce)
	}
	if req.NewClientOrderID != "" {
		params.Set("newClientOrderId", req.NewClientOrderID)
	}
	if req.ReduceOnly {
		params.Set("reduceOnly", "true")
	}

	var resp PlaceOrderResponse
	if err := c.signedRequest("POST", "/fapi/v1/order", params, &resp); err != nil {
		return nil, err
	}

	// 下单成功后写入ORDER_PLACED事件
	if c.reducer != nil {
		if err := c.reducer.WriteOrderPlaced(req.NewClientOrderID, req.Symbol, side, adjustedPrice, adjustedQty, req.ReduceOnly); err != nil {
			// 记录错误但不影响返回结果，因为订单已经下单成功
			fmt.Printf("Warning: failed to write order placed event: %v\n", err)
		}
	}

	return &resp, nil
}

// CancelOrder 撤单
func (c *Client) CancelOrder(req *CancelOrderRequest) (*PlaceOrderResponse, error) {
	params := make(url.Values)
	params.Set("symbol", req.Symbol)

	if req.OrderID > 0 {
		params.Set("orderId", strconv.FormatInt(req.OrderID, 10))
	}
	if req.OrigClientOrderID != "" {
		params.Set("origClientOrderId", req.OrigClientOrderID)
	}

	var resp PlaceOrderResponse
	if err := c.signedRequest("DELETE", "/fapi/v1/order", params, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// QueryOrder 查询订单
func (c *Client) QueryOrder(req *QueryOrderRequest) (*PlaceOrderResponse, error) {
	params := make(url.Values)
	params.Set("symbol", req.Symbol)

	if req.OrderID > 0 {
		params.Set("orderId", strconv.FormatInt(req.OrderID, 10))
	}
	if req.OrigClientOrderID != "" {
		params.Set("origClientOrderId", req.OrigClientOrderID)
	}

	var resp PlaceOrderResponse
	if err := c.signedRequest("GET", "/fapi/v1/order", params, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetOpenOrders 获取当前挂单
func (c *Client) GetOpenOrders(symbol string) ([]*PlaceOrderResponse, error) {
	params := make(url.Values)
	if symbol != "" {
		params.Set("symbol", symbol)
	}

	var resp []*PlaceOrderResponse
	if err := c.signedRequest("GET", "/fapi/v1/openOrders", params, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// GetUserTrades 获取用户成交历史
func (c *Client) GetUserTrades(symbol string, startTime, endTime int64, fromID int64, limit int) ([]*UserTrade, error) {
	params := make(url.Values)
	params.Set("symbol", symbol)

	if startTime > 0 {
		params.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		params.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if fromID > 0 {
		params.Set("fromId", strconv.FormatInt(fromID, 10))
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}

	var resp []*UserTrade
	if err := c.signedRequest("GET", "/fapi/v1/userTrades", params, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// GetPositionRisk 获取持仓风险
func (c *Client) GetPositionRisk(symbol string) ([]*types.Position, error) {
	params := make(url.Values)
	if symbol != "" {
		params.Set("symbol", symbol)
	}

	var resp []*types.Position
	if err := c.signedRequest("GET", "/fapi/v2/positionRisk", params, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// GetAccount 获取账户信息
func (c *Client) GetAccount() (*types.AccountInfo, error) {
	params := make(url.Values)

	var resp types.AccountInfo
	if err := c.signedRequest("GET", "/fapi/v2/account", params, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// GetAccountInfo 获取账户信息 (别名方法)
func (c *Client) GetAccountInfo() (*types.AccountInfo, error) {
	return c.GetAccount()
}

// GetTickerPrice 获取价格信息
func (c *Client) GetTickerPrice(symbol string) (*TickerPriceResponse, error) {
	params := make(url.Values)
	params.Set("symbol", symbol)
	
	var resp TickerPriceResponse
	if err := c.signedRequest("GET", "/fapi/v1/ticker/price", params, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetAccountTrades 获取账户成交记录
func (c *Client) GetAccountTrades(symbol string, startTime, endTime int64, fromId int64, limit int) ([]*UserTrade, error) {
	params := make(url.Values)
	
	if symbol != "" {
		params.Set("symbol", symbol)
	}
	if startTime > 0 {
		params.Set("startTime", strconv.FormatInt(startTime, 10))
	}
	if endTime > 0 {
		params.Set("endTime", strconv.FormatInt(endTime, 10))
	}
	if fromId > 0 {
		params.Set("fromId", strconv.FormatInt(fromId, 10))
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	
	var trades []*UserTrade
	if err := c.signedRequest("GET", "/fapi/v1/userTrades", params, &trades); err != nil {
		return nil, err
	}
	return trades, nil
}

// CreateListenKey 创建用户数据流监听密钥
func (c *Client) CreateListenKey() (*ListenKeyResponse, error) {
	var resp ListenKeyResponse
	if err := c.signedRequest("POST", "/fapi/v1/listenKey", make(url.Values), &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// KeepAliveListenKey 保活监听密钥
func (c *Client) KeepAliveListenKey(listenKey string) error {
	if listenKey == "" {
		return fmt.Errorf("listenKey cannot be empty")
	}
	
	params := make(url.Values)
	params.Set("listenKey", listenKey)
	
	// 使用PUT方法延长listenKey有效期
	err := c.signedRequest("PUT", "/fapi/v1/listenKey", params, nil)
	if err != nil {
		return fmt.Errorf("failed to keep alive listenKey: %w", err)
	}
	
	return nil
}

// DeleteListenKey 删除监听密钥
func (c *Client) DeleteListenKey() error {
	return c.signedRequest("DELETE", "/fapi/v1/listenKey", make(url.Values), nil)
}

// GetServerTime 获取服务器时间
func (c *Client) GetServerTime() (*ServerTimeResponse, error) {
	resp, err := c.httpClient.Get(c.baseURL + "/fapi/v1/time")
	if err != nil {
		return nil, fmt.Errorf("failed to get server time: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var serverTime ServerTimeResponse
	if err := json.Unmarshal(body, &serverTime); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server time: %w", err)
	}

	return &serverTime, nil
}

// signedRequest 发送签名请求
func (c *Client) signedRequest(method, endpoint string, params url.Values, result interface{}) error {
	// 记录请求开始时间
	startTime := time.Now()
	
	// 如果params为nil，初始化一个空的url.Values
	if params == nil {
		params = make(url.Values)
	}
	
	// 添加时间戳和接收窗口
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", strconv.Itoa(c.recvWindow))

	// 生成签名
	signature := c.sign(params.Encode())
	params.Set("signature", signature)

	// 构建请求
	var req *http.Request
	var err error

	if method == "GET" || method == "DELETE" {
		url := c.baseURL + endpoint + "?" + params.Encode()
		req, err = http.NewRequest(method, url, nil)
	} else {
		req, err = http.NewRequest(method, c.baseURL+endpoint, strings.NewReader(params.Encode()))
		if err == nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}

	if err != nil {
		// 记录失败的请求延迟
		latency := time.Since(startTime)
		if c.reducer != nil && c.reducer.GetMonitor() != nil {
			c.reducer.GetMonitor().RecordRequest(false, latency, types.ErrorCodeRequestBuildError)
		}
		return fmt.Errorf("failed to create request: %w", err)
	}

	// 设置请求头
	req.Header.Set("X-MBX-APIKEY", c.apiKey)

	// 发送请求
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// 记录失败的请求延迟
		latency := time.Since(startTime)
		if c.reducer != nil && c.reducer.GetMonitor() != nil {
			c.reducer.GetMonitor().RecordRequest(false, latency, types.ErrorCodeNetworkError)
		}
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		// 记录失败的请求延迟
		latency := time.Since(startTime)
		if c.reducer != nil && c.reducer.GetMonitor() != nil {
			c.reducer.GetMonitor().RecordRequest(false, latency, types.ErrorCodeResponseReadError)
		}
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// 计算请求延迟
	latency := time.Since(startTime)

	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		// 记录失败的请求延迟
		latency := time.Since(startTime)
		if c.reducer != nil && c.reducer.GetMonitor() != nil {
			c.reducer.GetMonitor().RecordRequest(false, latency, types.ErrorCode(fmt.Sprintf("HTTP_%d", resp.StatusCode)))
		}
		
		var apiErr APIError
		if json.Unmarshal(body, &apiErr) == nil {
			return &apiErr
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// 解析响应
	if result != nil {
		if err := json.Unmarshal(body, result); err != nil {
			// 记录失败的请求延迟
			if c.reducer != nil && c.reducer.GetMonitor() != nil {
				c.reducer.GetMonitor().RecordRequest(false, latency, types.ErrorCodeJSONParseError)
			}
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	// 记录成功的请求延迟
	if c.reducer != nil && c.reducer.GetMonitor() != nil {
		c.reducer.GetMonitor().RecordRequest(true, latency, types.ErrorCodeNone)
	}

	return nil
}

// sign 生成HMAC SHA256签名
func (c *Client) sign(message string) string {
	h := hmac.New(sha256.New, []byte(c.secretKey))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}

// UserTrade 用户成交记录
type UserTrade struct {
	Symbol          string `json:"symbol"`
	ID              int64  `json:"id"`
	OrderID         int64  `json:"orderId"`
	ClientOrderID   string `json:"clientOrderId"`
	Side            string `json:"side"`
	Price           string `json:"price"`
	Qty             string `json:"qty"`
	RealizedPnl     string `json:"realizedPnl"`
	MarginAsset     string `json:"marginAsset"`
	QuoteQty        string `json:"quoteQty"`
	Commission      string `json:"commission"`
	CommissionAsset string `json:"commissionAsset"`
	Time            int64  `json:"time"`
	PositionSide    string `json:"positionSide"`
	Buyer           bool   `json:"buyer"`
	Maker           bool   `json:"maker"`
}

// ListenKeyResponse 监听密钥响应
type ListenKeyResponse struct {
	ListenKey string `json:"listenKey"`
}

// ServerTimeResponse 服务器时间响应
type ServerTimeResponse struct {
	ServerTime int64 `json:"serverTime"`
}

// TickerPriceResponse 价格信息响应
type TickerPriceResponse struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

// APIError API错误响应
type APIError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Binance API error %d: %s", e.Code, e.Msg)
}

// GetExchangeInfo 获取交易所信息
func (c *Client) GetExchangeInfo() (*types.ExchangeInfo, error) {
	var result types.ExchangeInfo
	err := c.signedRequest("GET", "/fapi/v1/exchangeInfo", nil, &result)
	if err != nil {
		return nil, err
	}
	
	// 缓存交易规则
	c.rulesMutex.Lock()
	defer c.rulesMutex.Unlock()
	
	for _, symbol := range result.Symbols {
		if symbol.Status != "TRADING" {
			continue
		}
		
		rules := &types.TradingRules{
			Symbol:    symbol.Symbol,
			UpdatedAt: time.Now(),
		}
		
		// 解析过滤器
		for _, filter := range symbol.Filters {
			switch filter.FilterType {
			case "PRICE_FILTER":
				if filter.TickSize != "" {
					if tickSize, err := strconv.ParseFloat(filter.TickSize, 64); err == nil {
						rules.TickSize = tickSize
					}
				}
				if filter.MinPrice != "" {
					if minPrice, err := strconv.ParseFloat(filter.MinPrice, 64); err == nil {
						rules.MinPrice = minPrice
					}
				}
				if filter.MaxPrice != "" {
					if maxPrice, err := strconv.ParseFloat(filter.MaxPrice, 64); err == nil {
						rules.MaxPrice = maxPrice
					}
				}
			case "LOT_SIZE":
				if filter.StepSize != "" {
					if stepSize, err := strconv.ParseFloat(filter.StepSize, 64); err == nil {
						rules.StepSize = stepSize
					}
				}
				if filter.MinQty != "" {
					if minQty, err := strconv.ParseFloat(filter.MinQty, 64); err == nil {
						rules.MinQty = minQty
					}
				}
				if filter.MaxQty != "" {
					if maxQty, err := strconv.ParseFloat(filter.MaxQty, 64); err == nil {
						rules.MaxQty = maxQty
					}
				}
			case "MIN_NOTIONAL":
				if filter.MinNotional != "" {
					if minNotional, err := strconv.ParseFloat(filter.MinNotional, 64); err == nil {
						rules.MinNotional = minNotional
					}
				}
			}
		}
		
		c.tradingRules[symbol.Symbol] = rules
	}
	
	return &result, nil
}

// ValidateOrder 校验订单参数
func (c *Client) ValidateOrder(symbol string, side types.OrderSide, price, qty float64) *types.ValidationResult {
	c.rulesMutex.RLock()
	rules, exists := c.tradingRules[symbol]
	c.rulesMutex.RUnlock()
	
	if !exists {
		return &types.ValidationResult{
			Valid:  false,
			Reason: "交易规则未找到，请先调用GetExchangeInfo",
		}
	}
	
	result := &types.ValidationResult{Valid: true}
	
	// 价格精度校验和调整
	if rules.TickSize > 0 {
		if side == types.OrderSideBuy {
			// 买单向下取整到tick
			adjustedPrice := math.Floor(price/rules.TickSize) * rules.TickSize
			result.AdjustedPrice = adjustedPrice
		} else {
			// 卖单向上取整到tick
			adjustedPrice := math.Ceil(price/rules.TickSize) * rules.TickSize
			result.AdjustedPrice = adjustedPrice
		}
	} else {
		result.AdjustedPrice = price
	}
	
	// 数量精度校验和调整 - 强制向下取整
	if rules.StepSize > 0 {
		adjustedQty := math.Floor(qty/rules.StepSize) * rules.StepSize
		result.AdjustedQuantity = adjustedQty
	} else {
		result.AdjustedQuantity = qty
	}
	
	// 最小数量校验
	if result.AdjustedQuantity < rules.MinQty {
		result.Valid = false
		result.Reason = fmt.Sprintf("数量%.8f小于最小数量%.8f", result.AdjustedQuantity, rules.MinQty)
		return result
	}
	
	// 最大数量校验
	if rules.MaxQty > 0 && result.AdjustedQuantity > rules.MaxQty {
		result.Valid = false
		result.Reason = fmt.Sprintf("数量%.8f大于最大数量%.8f", result.AdjustedQuantity, rules.MaxQty)
		return result
	}
	
	// 价格范围校验
	if result.AdjustedPrice < rules.MinPrice {
		result.Valid = false
		result.Reason = fmt.Sprintf("价格%.8f小于最小价格%.8f", result.AdjustedPrice, rules.MinPrice)
		return result
	}
	
	if rules.MaxPrice > 0 && result.AdjustedPrice > rules.MaxPrice {
		result.Valid = false
		result.Reason = fmt.Sprintf("价格%.8f大于最大价格%.8f", result.AdjustedPrice, rules.MaxPrice)
		return result
	}
	
	// 最小名义价值校验 - 使用max(minQty, minNotional/计划价)的逻辑
	minRequiredQty := math.Max(rules.MinQty, rules.MinNotional/result.AdjustedPrice)
	
	if result.AdjustedQuantity < minRequiredQty {
		// 尝试调整数量以满足最小阈值
		requiredQty := minRequiredQty
		if rules.StepSize > 0 {
			requiredQty = math.Ceil(requiredQty/rules.StepSize) * rules.StepSize
		}
		
		// 检查调整后的数量是否超过最大限制
		if rules.MaxQty > 0 && requiredQty > rules.MaxQty {
			result.Valid = false
			result.Reason = fmt.Sprintf("无法满足最小阈值要求：需要数量%.8f，但超过最大数量%.8f", requiredQty, rules.MaxQty)
			return result
		}
		
		// 检查调整后的名义价值
		newNotional := result.AdjustedPrice * requiredQty
		if newNotional < rules.MinNotional {
			result.Valid = false
			result.Reason = fmt.Sprintf("名义价值%.8f小于最小名义价值%.8f，且无法通过调整数量满足", newNotional, rules.MinNotional)
			return result
		}
		
		result.AdjustedQuantity = requiredQty
	}
	
	// 最终验证：确保调整后的参数仍然满足所有约束
	finalNotional := result.AdjustedPrice * result.AdjustedQuantity
	if finalNotional < rules.MinNotional {
		result.Valid = false
		result.Reason = fmt.Sprintf("最终名义价值%.8f小于最小名义价值%.8f", finalNotional, rules.MinNotional)
		return result
	}
	
	// 精度验证：确保价格和数量已正确对齐到tick/step
	if rules.TickSize > 0 {
		priceRemainder := math.Mod(result.AdjustedPrice, rules.TickSize)
		if math.Abs(priceRemainder) > 1e-8 && math.Abs(priceRemainder-rules.TickSize) > 1e-8 {
			result.Valid = false
			result.Reason = fmt.Sprintf("价格%.8f未正确对齐到tick size %.8f", result.AdjustedPrice, rules.TickSize)
			return result
		}
	}
	
	if rules.StepSize > 0 {
		qtyRemainder := math.Mod(result.AdjustedQuantity, rules.StepSize)
		if math.Abs(qtyRemainder) > 1e-8 {
			result.Valid = false
			result.Reason = fmt.Sprintf("数量%.8f未正确对齐到step size %.8f", result.AdjustedQuantity, rules.StepSize)
			return result
		}
	}
	
	return result
}

// GetTradingRules 获取交易规则
func (c *Client) GetTradingRules(symbol string) (*types.TradingRules, bool) {
	c.rulesMutex.RLock()
	defer c.rulesMutex.RUnlock()
	
	rules, exists := c.tradingRules[symbol]
	return rules, exists
}
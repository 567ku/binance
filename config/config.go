package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config 系统配置结构
type Config struct {
	API     APIConfig     `json:"api"`
	Trading TradingConfig `json:"trading"`
	System  SystemConfig  `json:"system"`
}

// APIConfig API配置
type APIConfig struct {
	BaseURL string `json:"base_url"`
	WsURL   string `json:"ws_url"`
	KeyFile string `json:"key_file"`
}

// TradingConfig 交易配置
type TradingConfig struct {
	Symbol       string      `json:"symbol"`
	PositionSide string      `json:"position_side"`
	MinOrderQty  float64     `json:"min_order_qty"`
	Grids        GridsConfig `json:"grids"`
}

// GridsConfig 多网格配置
type GridsConfig struct {
	XSegment GridConfig `json:"x_segment"`
	ASegment GridConfig `json:"a_segment"`
	BSegment GridConfig `json:"b_segment"`
}

// GridConfig 网格配置
type GridConfig struct {
	Enabled       bool    `json:"enabled"`
	PriceMin      float64 `json:"price_min"`
	PriceMax      float64 `json:"price_max"`
	StepSize      float64 `json:"step_size"`
	OrderQty      float64 `json:"order_qty"`
	MaxBuyOrders  int     `json:"max_buy_orders"`
	MaxSellOrders int     `json:"max_sell_orders"`
}

// SystemConfig 系统配置
type SystemConfig struct {
	DataDir                 string `json:"data_dir"`
	LogLevel                string `json:"log_level"`
	SnapshotInterval        int    `json:"snapshot_interval"`
	MaxEventsBeforeSnapshot int    `json:"max_events_before_snapshot"`
	WSPingInterval          int    `json:"ws_ping_interval"`
	WSTimeout               int    `json:"ws_timeout"`
	DegradedPollInterval    int    `json:"degraded_poll_interval"`
	RecvWindow              int    `json:"recv_window"`
}

// APICredentials API凭证
type APICredentials struct {
	APIKey    string
	SecretKey string
}

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	return &config, nil
}

// LoadAPICredentials 从密钥文件加载API凭证
func LoadAPICredentials(keyFile string) (*APICredentials, error) {
	file, err := os.Open(keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open key file: %w", err)
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read key file: %w", err)
	}

	if len(lines) < 2 {
		return nil, fmt.Errorf("key file must contain at least 2 lines (API key and secret)")
	}

	return &APICredentials{
		APIKey:    lines[0],
		SecretKey: lines[1],
	}, nil
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.API.BaseURL == "" {
		return fmt.Errorf("API base URL is required")
	}
	if c.API.WsURL == "" {
		return fmt.Errorf("WebSocket URL is required")
	}
	if c.API.KeyFile == "" {
		return fmt.Errorf("API key file is required")
	}
	if c.Trading.Symbol == "" {
		return fmt.Errorf("trading symbol is required")
	}
	if c.Trading.MinOrderQty <= 0 {
		return fmt.Errorf("minimum order quantity must be positive")
	}
	// 验证网格配置
	if c.Trading.Grids.XSegment.Enabled {
		if err := c.validateGridConfig(&c.Trading.Grids.XSegment); err != nil {
			return fmt.Errorf("X segment grid config error: %w", err)
		}
	}
	if c.Trading.Grids.ASegment.Enabled {
		if err := c.validateGridConfig(&c.Trading.Grids.ASegment); err != nil {
			return fmt.Errorf("A segment grid config error: %w", err)
		}
	}
	if c.Trading.Grids.BSegment.Enabled {
		if err := c.validateGridConfig(&c.Trading.Grids.BSegment); err != nil {
			return fmt.Errorf("B segment grid config error: %w", err)
		}
	}
	if c.System.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	return nil
}

// validateGridConfig 验证单个网格配置
func (c *Config) validateGridConfig(grid *GridConfig) error {
	if grid.PriceMin >= grid.PriceMax {
		return fmt.Errorf("price min must be less than price max")
	}
	if grid.StepSize <= 0 {
		return fmt.Errorf("step size must be positive")
	}
	if grid.OrderQty <= 0 {
		return fmt.Errorf("order quantity must be positive")
	}
	if grid.MaxBuyOrders <= 0 || grid.MaxSellOrders <= 0 {
		return fmt.Errorf("max orders must be positive")
	}
	return nil
}
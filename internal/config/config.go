package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Version string        `yaml:"version"`
	JobName string        `yaml:"job_name"`
	Source  DBConfig      `yaml:"source"`
	Target  DBConfig      `yaml:"target"`
	Sync    SyncConfig    `yaml:"sync"`
	Tables  []TableConfig `yaml:"tables"`
	Monitor MonitorConfig `yaml:"monitor"`
	Compare CompareConfig `yaml:"compare"`
	Schema  SchemaConfig  `yaml:"schema"`
}

type DBConfig struct {
	Host            string            `yaml:"host"`
	Port            int               `yaml:"port"`
	Database        string            `yaml:"database"`
	Username        string            `yaml:"username"`
	Password        string            `yaml:"password"`
	Params          map[string]string `yaml:"params"`
	MaxOpenConns    int               `yaml:"max_open_conns"`
	MaxIdleConns    int               `yaml:"max_idle_conns"`
	ConnMaxLifetime string            `yaml:"conn_max_lifetime"`
}

type SyncConfig struct {
	Workers       int    `yaml:"workers"`
	TableWorkers  int    `yaml:"table_workers"`
	BatchSize     int    `yaml:"batch_size"`
	ReadBuffer    int    `yaml:"read_buffer"`
	ChannelBuffer int    `yaml:"channel_buffer"`
	MaxRetries    int    `yaml:"max_retries"`
	RetryInterval string `yaml:"retry_interval"`
}

type TableConfig struct {
	Source             string          `yaml:"source"`
	Target             string          `yaml:"target"`
	Columns            []ColumnMapping `yaml:"columns"`
	SplitColumn        string          `yaml:"split_column"`
	Where              string          `yaml:"where"`
	TruncateBeforeSync bool            `yaml:"truncate_before_sync"`
}

type ColumnMapping struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

type MonitorConfig struct {
	ReportInterval string `yaml:"report_interval"`
	LogLevel       string `yaml:"log_level"`
}

type CompareConfig struct {
	AutoCompare    bool `yaml:"auto_compare"`
	CompareWorkers int  `yaml:"compare_workers"`
	CountOnly      bool `yaml:"count_only"`
	BatchKeys      int  `yaml:"batch_keys"`
}

type SchemaConfig struct {
	StripAutoIncrement bool `yaml:"strip_auto_increment"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file failed: %w", err)
	}

	data = expandEnvVars(data)

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config failed: %w", err)
	}

	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func expandEnvVars(data []byte) []byte {
	re := regexp.MustCompile(`\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)
	return re.ReplaceAllFunc(data, func(match []byte) []byte {
		var varName string
		if len(match) > 2 && match[1] == '{' {
			varName = string(match[2 : len(match)-1])
		} else {
			varName = string(match[1:])
		}
		return []byte(os.Getenv(varName))
	})
}

func (c *Config) setDefaults() {
	if c.Sync.Workers == 0 {
		c.Sync.Workers = 64
	}
	if c.Sync.TableWorkers == 0 {
		c.Sync.TableWorkers = 1
	}
	if c.Sync.BatchSize == 0 {
		c.Sync.BatchSize = 2000
	}
	if c.Sync.ReadBuffer == 0 {
		c.Sync.ReadBuffer = 5000
	}
	if c.Sync.ChannelBuffer == 0 {
		c.Sync.ChannelBuffer = 10000
	}
	if c.Sync.MaxRetries == 0 {
		c.Sync.MaxRetries = 3
	}
	if c.Sync.RetryInterval == "" {
		c.Sync.RetryInterval = "5s"
	}
	if c.Monitor.ReportInterval == "" {
		c.Monitor.ReportInterval = "10s"
	}
	if c.Monitor.LogLevel == "" {
		c.Monitor.LogLevel = "info"
	}
	if c.Compare.CompareWorkers == 0 {
		c.Compare.CompareWorkers = 10
	}
	if c.Compare.BatchKeys == 0 {
		c.Compare.BatchKeys = 1000
	}
	if c.Source.MaxOpenConns == 0 {
		c.Source.MaxOpenConns = 20
	}
	if c.Source.MaxIdleConns == 0 {
		c.Source.MaxIdleConns = 5
	}
	if c.Target.MaxOpenConns == 0 {
		c.Target.MaxOpenConns = 50
	}
	if c.Target.MaxIdleConns == 0 {
		c.Target.MaxIdleConns = 10
	}
}

func (c *Config) validate() error {
	if c.Source.Host == "" {
		return fmt.Errorf("source host is required")
	}
	if c.Target.Host == "" {
		return fmt.Errorf("target host is required")
	}
	if c.Source.Port == 0 {
		c.Source.Port = 3306
	}
	if c.Target.Port == 0 {
		c.Target.Port = 2881
	}
	if c.Source.Database == "" {
		return fmt.Errorf("source database is required")
	}
	if c.Target.Database == "" {
		return fmt.Errorf("target database is required")
	}
	return nil
}

func (c *Config) GetRetryInterval() time.Duration {
	d, _ := time.ParseDuration(c.Sync.RetryInterval)
	if d == 0 {
		return 5 * time.Second
	}
	return d
}

func (c *Config) GetReportInterval() time.Duration {
	d, _ := time.ParseDuration(c.Monitor.ReportInterval)
	if d == 0 {
		return 10 * time.Second
	}
	return d
}

func (c *DBConfig) GetConnMaxLifetime() time.Duration {
	d, _ := time.ParseDuration(c.ConnMaxLifetime)
	if d == 0 {
		return time.Hour
	}
	return d
}

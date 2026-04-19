package database

import (
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/huayumicheng/MySQL2OBSync/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

type Connection struct {
	DB     *sql.DB
	Config config.DBConfig
}

func NewMySQLConnection(cfg config.DBConfig) (*Connection, error) {
	params := make(map[string]string)
	params["charset"] = "utf8mb4"
	params["parseTime"] = "true"
	params["loc"] = "UTC"

	for k, v := range cfg.Params {
		params[k] = v
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	if len(params) > 0 {
		dsn += "?"
		first := true
		for k, v := range params {
			if !first {
				dsn += "&"
			}
			dsn += fmt.Sprintf("%s=%s", k, url.QueryEscape(v))
			first = false
		}
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql connection failed: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.GetConnMaxLifetime())

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping mysql failed: %w", err)
	}

	return &Connection{DB: db, Config: cfg}, nil
}

func (c *Connection) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}

func (c *Connection) Ping() error { return c.DB.Ping() }
func (c *Connection) GetStats() sql.DBStats { return c.DB.Stats() }

func ExecuteInTransaction(db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func RetryOperation(maxRetries int, interval time.Duration, operation func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = operation()
		if err == nil {
			return nil
		}
		if i < maxRetries-1 {
			time.Sleep(interval)
		}
	}
	return err
}

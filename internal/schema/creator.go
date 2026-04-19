package schema

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/huayumicheng/MySQL2OBSync/internal/logger"
)

type Creator struct {
	sourceDB          *sql.DB
	targetDB          *sql.DB
	stripAutoIncrement bool
}

func NewCreator(sourceDB, targetDB *sql.DB, stripAutoIncrement bool) *Creator {
	return &Creator{sourceDB: sourceDB, targetDB: targetDB, stripAutoIncrement: stripAutoIncrement}
}

func (c *Creator) GetCreateTableSQL(table string) (string, error) {
	q := fmt.Sprintf("SHOW CREATE TABLE %s", quoteShowName(table))
	var tableName string
	var ddl string
	if err := c.sourceDB.QueryRow(q).Scan(&tableName, &ddl); err != nil {
		return "", err
	}
	return ddl, nil
}

func (c *Creator) NormalizeCreateTableSQL(ddl string) string {
	out := ddl
	out = strings.Replace(out, "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
	if c.stripAutoIncrement {
		re := regexp.MustCompile(`(?i)\sAUTO_INCREMENT=\d+`)
		out = re.ReplaceAllString(out, "")
	}
	return out
}

func (c *Creator) CreateTable(table string, dryRun bool) (string, error) {
	ddl, err := c.GetCreateTableSQL(table)
	if err != nil {
		return "", fmt.Errorf("get create table sql failed: %w", err)
	}
	ddl = c.NormalizeCreateTableSQL(ddl)
	if dryRun {
		return ddl, nil
	}
	if _, err := c.targetDB.Exec(ddl); err != nil {
		return ddl, fmt.Errorf("create table failed: %w", err)
	}
	return ddl, nil
}

func (c *Creator) CreateSchemaForTables(tables []string, dryRun bool) error {
	for _, t := range tables {
		logger.Info("Creating table: %s", t)
		ddl, err := c.CreateTable(t, dryRun)
		if err != nil {
			return fmt.Errorf("create table %s failed: %w", t, err)
		}
		if dryRun {
			logger.Info("Table SQL:\n%s;\n", ddl)
		} else {
			logger.Info("  ✓ Table created")
		}
	}
	return nil
}

func (c *Creator) GenerateCreateTableScript(tables []string) (string, error) {
	var sb strings.Builder
	for _, t := range tables {
		ddl, err := c.GetCreateTableSQL(t)
		if err != nil {
			return "", err
		}
		ddl = c.NormalizeCreateTableSQL(ddl)
		sb.WriteString(fmt.Sprintf("-- Table: %s\n", t))
		sb.WriteString(ddl)
		sb.WriteString(";\n\n")
	}
	return sb.String(), nil
}

func (c *Creator) SaveCreateTableScript(tables []string, filename string) error {
	s, err := c.GenerateCreateTableScript(tables)
	if err != nil {
		return err
	}
	return os.WriteFile(filename, []byte(s), 0644)
}

func quoteShowName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "``"
	}
	parts := strings.Split(name, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		p = strings.ReplaceAll(p, "`", "``")
		out = append(out, "`"+p+"`")
	}
	if len(out) == 0 {
		return "``"
	}
	return strings.Join(out, ".")
}

package database

import (
	"database/sql"
	"fmt"
)

func GetTableRowCount(db *sql.DB, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", QuoteTable(table))
	var count int64
	err := db.QueryRow(query).Scan(&count)
	return count, err
}

func TruncateTable(db *sql.DB, table string) error {
	query := fmt.Sprintf("TRUNCATE TABLE %s", QuoteTable(table))
	_, err := db.Exec(query)
	return err
}

func GetPrimaryKeyRange(db *sql.DB, table, pkColumn string) (min, max int64, ok bool, err error) {
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		QuoteIdent(pkColumn),
		QuoteIdent(pkColumn),
		QuoteTable(table),
	)
	row := db.QueryRow(query)
	var minVal, maxVal sql.NullInt64
	if err = row.Scan(&minVal, &maxVal); err != nil {
		return 0, 0, false, err
	}
	if !minVal.Valid || !maxVal.Valid {
		return 0, 0, false, nil
	}
	return minVal.Int64, maxVal.Int64, true, nil
}


package schema

import (
	"database/sql"
	"fmt"
	"strings"
)

type Discovery struct {
	db *sql.DB
}

func NewDiscovery(db *sql.DB) *Discovery {
	return &Discovery{db: db}
}

func (d *Discovery) DiscoverAllTables() ([]string, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func (d *Discovery) DiscoverTable(table string) (*TableSchema, error) {
	tableName := strings.TrimSpace(table)
	if tableName == "" {
		return nil, fmt.Errorf("empty table name")
	}

	ts := &TableSchema{
		TableName: tableName,
		Columns:   make([]ColumnSchema, 0),
	}

	colQuery := `
		SELECT
			column_name,
			column_type,
			data_type,
			is_nullable,
			column_default,
			extra,
			column_comment
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		ORDER BY ordinal_position
	`
	rows, err := d.db.Query(colQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var col ColumnSchema
		var isNullable string
		var def sql.NullString
		if err := rows.Scan(
			&col.Name,
			&col.ColumnType,
			&col.DataType,
			&isNullable,
			&def,
			&col.Extra,
			&col.Comment,
		); err != nil {
			return nil, err
		}
		col.IsNullable = strings.EqualFold(isNullable, "YES")
		if def.Valid {
			v := def.String
			col.DefaultValue = &v
		}
		ts.Columns = append(ts.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	pkQuery := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position
	`
	pkRows, err := d.db.Query(pkQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var c string
		if err := pkRows.Scan(&c); err != nil {
			return nil, err
		}
		ts.PrimaryKeys = append(ts.PrimaryKeys, c)
	}
	if err := pkRows.Err(); err != nil {
		return nil, err
	}
	if len(ts.PrimaryKeys) > 0 {
		ts.PrimaryKey = ts.PrimaryKeys[0]
	}

	return ts, nil
}

func (d *Discovery) DiscoverExtendedTable(table string) (*ExtendedTableSchema, error) {
	base, err := d.DiscoverTable(table)
	if err != nil {
		return nil, err
	}

	ets := &ExtendedTableSchema{
		TableSchema: *base,
		Indexes:     make([]IndexSchema, 0),
		Constraints: make([]ConstraintSchema, 0),
	}

	tableCommentQuery := `
		SELECT table_comment
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_name = ?
	`
	_ = d.db.QueryRow(tableCommentQuery, ets.TableName).Scan(&ets.TableComment)

	idxQuery := `
		SELECT index_name, non_unique, column_name
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		ORDER BY index_name, seq_in_index
	`
	idxRows, err := d.db.Query(idxQuery, ets.TableName)
	if err != nil {
		return nil, err
	}
	defer idxRows.Close()

	indexMap := make(map[string]*IndexSchema)
	for idxRows.Next() {
		var idxName, colName string
		var nonUnique int
		if err := idxRows.Scan(&idxName, &nonUnique, &colName); err != nil {
			return nil, err
		}
		isPrimary := idxName == "PRIMARY"
		isUnique := nonUnique == 0
		if s, ok := indexMap[idxName]; ok {
			s.Columns = append(s.Columns, colName)
		} else {
			indexMap[idxName] = &IndexSchema{
				Name:      idxName,
				IsUnique:  isUnique,
				IsPrimary: isPrimary,
				Columns:   []string{colName},
			}
		}
	}
	if err := idxRows.Err(); err != nil {
		return nil, err
	}
	for _, v := range indexMap {
		ets.Indexes = append(ets.Indexes, *v)
	}

	fkQuery := `
		SELECT
			kcu.constraint_name,
			kcu.column_name,
			kcu.referenced_table_name,
			kcu.referenced_column_name,
			kcu.ordinal_position
		FROM information_schema.key_column_usage kcu
		WHERE kcu.table_schema = DATABASE()
		  AND kcu.table_name = ?
		  AND kcu.referenced_table_name IS NOT NULL
		ORDER BY kcu.constraint_name, kcu.ordinal_position
	`
	fkRows, err := d.db.Query(fkQuery, ets.TableName)
	if err != nil {
		return nil, err
	}
	defer fkRows.Close()

	fkMap := make(map[string]*ConstraintSchema)
	for fkRows.Next() {
		var name, col, refTable, refCol string
		var ord int
		if err := fkRows.Scan(&name, &col, &refTable, &refCol, &ord); err != nil {
			return nil, err
		}
		_ = ord
		if c, ok := fkMap[name]; ok {
			c.Columns = append(c.Columns, col)
			c.RefColumns = append(c.RefColumns, refCol)
		} else {
			fkMap[name] = &ConstraintSchema{
				Name:       name,
				Type:       "FOREIGN KEY",
				Columns:    []string{col},
				RefTable:   refTable,
				RefColumns: []string{refCol},
			}
		}
	}
	if err := fkRows.Err(); err != nil {
		return nil, err
	}
	for _, v := range fkMap {
		ets.Constraints = append(ets.Constraints, *v)
	}

	return ets, nil
}


package compare

import (
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huayumicheng/MySQL2OBSync/internal/database"
	"github.com/huayumicheng/MySQL2OBSync/internal/logger"
)

type Comparator struct {
	sourceDB *database.Connection
	targetDB *database.Connection
	workers  int
}

func NewComparator(sourceDB, targetDB *database.Connection, workers int) *Comparator {
	if workers <= 0 {
		workers = 10
	}
	return &Comparator{sourceDB: sourceDB, targetDB: targetDB, workers: workers}
}

type ColumnMinMax struct {
	ColumnName string
	SourceMin  interface{}
	SourceMax  interface{}
	TargetMin  interface{}
	TargetMax  interface{}
	MinMatch   bool
	MaxMatch   bool
}

type CompareResult struct {
	SourceTable      string
	TargetTable      string
	SourceCount      int64
	TargetCount      int64
	CountMatch       bool
	SampledRows      int64
	MatchedRows      int64
	MismatchedRows   int64
	SourceOnlyRows   int64
	TargetOnlyRows   int64
	Errors           []string
	SampleMismatches []MismatchDetail
	NoPKUK           bool
	ColumnStats      []ColumnMinMax
}

type MismatchDetail struct {
	Key        string
	KeyColumns []string
	KeyType    string
	SourceData map[string]interface{}
	TargetData map[string]interface{}
	DiffCols   []string
}

type TablePair struct {
	Source     string
	Target     string
	SampleRate float64
	CountOnly  bool
}

func (c *Comparator) CompareTables(tables []TablePair) ([]CompareResult, error) {
	results := make([]CompareResult, 0, len(tables))
	var mu sync.Mutex
	var wg sync.WaitGroup

	sem := make(chan struct{}, c.workers)

	for _, table := range tables {
		wg.Add(1)
		go func(t TablePair) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			result, err := c.CompareTable(t.Source, t.Target, t.SampleRate, t.CountOnly)
			if err != nil {
				result.Errors = append(result.Errors, err.Error())
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(table)
	}

	wg.Wait()
	return results, nil
}

func (c *Comparator) CompareTable(sourceTable, targetTable string, sampleRate float64, countOnly bool) (CompareResult, error) {
	result := CompareResult{SourceTable: sourceTable, TargetTable: targetTable}

	logger.Info("\nComparing table: %s <-> %s", sourceTable, targetTable)

	sourceCount, targetCount, countMatch, err := c.compareRowCount(sourceTable, targetTable)
	if err != nil {
		return result, err
	}
	result.SourceCount = sourceCount
	result.TargetCount = targetCount
	result.CountMatch = countMatch

	logger.Info("  [%s] Source count: %d, Target count: %d, Match: %v", sourceTable, sourceCount, targetCount, countMatch)
	if !countMatch {
		logger.Warn("  WARNING: Row count mismatch!")
	}
	if countOnly {
		logger.Info("  [%s] Count-only mode, skipping data content comparison", sourceTable)
		return result, nil
	}

	if sampleRate > 0 && sourceCount > 0 {
		sampleResult, err := c.sampleCompare(sourceTable, targetTable, sampleRate)
		if err != nil {
			result.Errors = append(result.Errors, err.Error())
		} else {
			result.SampledRows = sampleResult.SampledRows
			result.MatchedRows = sampleResult.MatchedRows
			result.MismatchedRows = sampleResult.MismatchedRows
			result.SourceOnlyRows = sampleResult.SourceOnlyRows
			result.TargetOnlyRows = sampleResult.TargetOnlyRows
			result.SampleMismatches = sampleResult.SampleMismatches
			result.NoPKUK = sampleResult.NoPKUK
			result.ColumnStats = sampleResult.ColumnStats
			if sampleResult.NoPKUK {
				logger.Info("  [%s] [NO_PK_UK] No primary/unique key - comparing row counts and column min/max only", sourceTable)
			} else {
				logger.Info("  [%s] Sampled: %d, Matched: %d, Mismatched: %d", sourceTable, sampleResult.SampledRows, sampleResult.MatchedRows, sampleResult.MismatchedRows)
			}
		}
	}

	return result, nil
}

func (c *Comparator) compareRowCount(sourceTable, targetTable string) (int64, int64, bool, error) {
	sourceCount, err := database.GetTableRowCount(c.sourceDB.DB, sourceTable)
	if err != nil {
		return 0, 0, false, fmt.Errorf("get source row count failed: %w", err)
	}
	targetCount, err := database.GetTableRowCount(c.targetDB.DB, targetTable)
	if err != nil {
		return sourceCount, 0, false, fmt.Errorf("get target row count failed: %w", err)
	}
	return sourceCount, targetCount, sourceCount == targetCount, nil
}

type SampleResult struct {
	SampledRows      int64
	MatchedRows      int64
	MismatchedRows   int64
	SourceOnlyRows   int64
	TargetOnlyRows   int64
	SampleMismatches []MismatchDetail
	NoPKUK           bool
	ColumnStats      []ColumnMinMax
}

func (c *Comparator) sampleCompare(sourceTable, targetTable string, sampleRate float64) (SampleResult, error) {
	result := SampleResult{}

	keyColumns, keyType, err := c.getUniqueKey(sourceTable)
	if err != nil {
		return c.noPKUKCompare(sourceTable, targetTable)
	}

	sampleKeys, err := c.getSampleKeys(sourceTable, keyColumns, sampleRate)
	if err != nil {
		return result, err
	}
	result.SampledRows = int64(len(sampleKeys))
	if len(sampleKeys) == 0 {
		return result, nil
	}

	var matched, mismatched, sourceOnly, targetOnly int64
	var mismatchesMu sync.Mutex
	var sampleMismatches []MismatchDetail

	tasks := make(chan []interface{}, len(sampleKeys))
	var wg sync.WaitGroup

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for keyValues := range tasks {
				match, srcOnly, tgtOnly, diffCols, srcData, tgtData, err := c.compareRowByKey(sourceTable, targetTable, keyColumns, keyValues)
				if err != nil {
					continue
				}
				if srcOnly {
					atomic.AddInt64(&sourceOnly, 1)
					continue
				}
				if tgtOnly {
					atomic.AddInt64(&targetOnly, 1)
					continue
				}
				if match {
					atomic.AddInt64(&matched, 1)
				} else {
					atomic.AddInt64(&mismatched, 1)
					if len(sampleMismatches) < 5 {
						mismatchesMu.Lock()
						if len(sampleMismatches) < 5 {
							keyStr := buildKeyString(keyColumns, keyValues)
							sampleMismatches = append(sampleMismatches, MismatchDetail{
								Key:        keyStr,
								KeyColumns: keyColumns,
								KeyType:    keyType,
								SourceData: srcData,
								TargetData: tgtData,
								DiffCols:   diffCols,
							})
						}
						mismatchesMu.Unlock()
					}
				}
			}
		}()
	}

	for _, key := range sampleKeys {
		tasks <- key
	}
	close(tasks)
	wg.Wait()

	result.MatchedRows = matched
	result.MismatchedRows = mismatched
	result.SourceOnlyRows = sourceOnly
	result.TargetOnlyRows = targetOnly
	result.SampleMismatches = sampleMismatches

	return result, nil
}

func (c *Comparator) getUniqueKey(table string) ([]string, string, error) {
	pkCols, err := c.getPrimaryKeyColumns(c.sourceDB.DB, table)
	if err == nil && len(pkCols) > 0 {
		return pkCols, "PK", nil
	}
	ukCols, err := c.getFirstUniqueIndexColumns(c.sourceDB.DB, table)
	if err != nil {
		return nil, "", err
	}
	if len(ukCols) == 0 {
		return nil, "", fmt.Errorf("no unique key found")
	}
	return ukCols, "UK", nil
}

func (c *Comparator) getPrimaryKeyColumns(db *sql.DB, table string) ([]string, error) {
	q := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND constraint_name = 'PRIMARY'
		ORDER BY ordinal_position
	`
	rows, err := db.Query(q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func (c *Comparator) getFirstUniqueIndexColumns(db *sql.DB, table string) ([]string, error) {
	q := `
		SELECT index_name, column_name, seq_in_index
		FROM information_schema.statistics
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND non_unique = 0
		  AND index_name <> 'PRIMARY'
		ORDER BY index_name, seq_in_index
	`
	rows, err := db.Query(q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type item struct {
		col string
		seq int
	}
	indexCols := map[string][]item{}
	for rows.Next() {
		var idx, col string
		var seq int
		if err := rows.Scan(&idx, &col, &seq); err != nil {
			return nil, err
		}
		indexCols[idx] = append(indexCols[idx], item{col: col, seq: seq})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(indexCols) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(indexCols))
	for n := range indexCols {
		names = append(names, n)
	}
	sort.Strings(names)
	chosen := names[0]
	items := indexCols[chosen]
	sort.Slice(items, func(i, j int) bool { return items[i].seq < items[j].seq })
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it.col)
	}
	return out, nil
}

func (c *Comparator) getSampleKeys(table string, keyColumns []string, sampleRate float64) ([][]interface{}, error) {
	totalRows := c.getApproxTableRows(c.sourceDB.DB, table)
	if totalRows <= 0 {
		cnt, err := database.GetTableRowCount(c.sourceDB.DB, table)
		if err == nil {
			totalRows = cnt
		} else {
			totalRows = 100000
		}
	}

	sampleSize := int(float64(totalRows) * sampleRate)
	if sampleSize < 1 {
		sampleSize = 1
	}
	if sampleSize > 10000 {
		sampleSize = 10000
	}

	colList := make([]string, 0, len(keyColumns))
	for _, c := range keyColumns {
		colList = append(colList, database.QuoteIdent(c))
	}

	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY RAND() LIMIT %d",
		strings.Join(colList, ", "),
		database.QuoteTable(table),
		sampleSize,
	)

	rows, err := c.sourceDB.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sampleKeys := make([][]interface{}, 0, sampleSize)
	for rows.Next() {
		keyValues := make([]interface{}, len(keyColumns))
		valuePtrs := make([]interface{}, len(keyColumns))
		for i := range keyValues {
			valuePtrs[i] = &keyValues[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}
		sampleKeys = append(sampleKeys, keyValues)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rand.Shuffle(len(sampleKeys), func(i, j int) {
		sampleKeys[i], sampleKeys[j] = sampleKeys[j], sampleKeys[i]
	})

	return sampleKeys, nil
}

func (c *Comparator) getApproxTableRows(db *sql.DB, table string) int64 {
	q := `
		SELECT table_rows
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_name = ?
	`
	var rows sql.NullInt64
	if err := db.QueryRow(q, table).Scan(&rows); err != nil {
		return 0
	}
	if rows.Valid {
		return rows.Int64
	}
	return 0
}

func (c *Comparator) compareRowByKey(sourceTable, targetTable string, keyColumns []string, keyValues []interface{}) (match, sourceOnly, targetOnly bool, diffCols []string, sourceData, targetData map[string]interface{}, err error) {
	where := make([]string, 0, len(keyColumns))
	for _, col := range keyColumns {
		where = append(where, fmt.Sprintf("%s = ?", database.QuoteIdent(col)))
	}
	whereClause := strings.Join(where, " AND ")

	srcRow, err := fetchOneRow(c.sourceDB.DB, sourceTable, whereClause, keyValues)
	if err != nil {
		return false, false, false, nil, nil, nil, err
	}
	tgtRow, err := fetchOneRow(c.targetDB.DB, targetTable, whereClause, keyValues)
	if err != nil {
		return false, false, false, nil, nil, nil, err
	}

	if srcRow == nil && tgtRow == nil {
		return true, false, false, nil, nil, nil, nil
	}
	if srcRow != nil && tgtRow == nil {
		return false, true, false, nil, srcRow, nil, nil
	}
	if srcRow == nil && tgtRow != nil {
		return false, false, true, nil, nil, tgtRow, nil
	}

	diff := compareRowMaps(srcRow, tgtRow)
	return len(diff) == 0, false, false, diff, srcRow, tgtRow, nil
}

func fetchOneRow(db *sql.DB, table string, whereClause string, args []interface{}) (map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1", database.QuoteTable(table), whereClause)
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, nil
	}
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	m := make(map[string]interface{}, len(cols))
	for i, c := range cols {
		m[c] = normalizeValue(values[i])
	}
	return m, nil
}

func compareRowMaps(a, b map[string]interface{}) []string {
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[strings.ToLower(k)] = struct{}{}
	}
	for k := range b {
		keys[strings.ToLower(k)] = struct{}{}
	}
	var diff []string
	for k := range keys {
		av, aok := getByCI(a, k)
		bv, bok := getByCI(b, k)
		if !aok || !bok {
			diff = append(diff, k)
			continue
		}
		if !reflect.DeepEqual(av, bv) {
			diff = append(diff, k)
		}
	}
	sort.Strings(diff)
	return diff
}

func getByCI(m map[string]interface{}, keyLower string) (interface{}, bool) {
	for k, v := range m {
		if strings.EqualFold(k, keyLower) {
			return v, true
		}
	}
	return nil, false
}

func normalizeValue(v interface{}) interface{} {
	switch x := v.(type) {
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05.000000")
	default:
		return v
	}
}

func buildKeyString(cols []string, vals []interface{}) string {
	parts := make([]string, 0, len(cols))
	for i := range cols {
		if i < len(vals) {
			parts = append(parts, fmt.Sprintf("%s=%v", cols[i], normalizeValue(vals[i])))
		}
	}
	return strings.Join(parts, ",")
}

func (c *Comparator) noPKUKCompare(sourceTable, targetTable string) (SampleResult, error) {
	res := SampleResult{NoPKUK: true}

	cols, err := c.getTableColumns(c.sourceDB.DB, sourceTable)
	if err != nil {
		return res, err
	}

	for _, col := range cols {
		srcMin, srcMax, err1 := getMinMax(c.sourceDB.DB, sourceTable, col)
		tgtMin, tgtMax, err2 := getMinMax(c.targetDB.DB, targetTable, col)
		if err1 != nil || err2 != nil {
			continue
		}
		srcMinN := normalizeValue(srcMin)
		srcMaxN := normalizeValue(srcMax)
		tgtMinN := normalizeValue(tgtMin)
		tgtMaxN := normalizeValue(tgtMax)
		res.ColumnStats = append(res.ColumnStats, ColumnMinMax{
			ColumnName: col,
			SourceMin:  srcMinN,
			SourceMax:  srcMaxN,
			TargetMin:  tgtMinN,
			TargetMax:  tgtMaxN,
			MinMatch:   reflect.DeepEqual(srcMinN, tgtMinN),
			MaxMatch:   reflect.DeepEqual(srcMaxN, tgtMaxN),
		})
	}

	return res, nil
}

func (c *Comparator) getTableColumns(db *sql.DB, table string) ([]string, error) {
	q := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		ORDER BY ordinal_position
	`
	rows, err := db.Query(q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func getMinMax(db *sql.DB, table string, col string) (interface{}, interface{}, error) {
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s",
		database.QuoteIdent(col),
		database.QuoteIdent(col),
		database.QuoteTable(table),
	)
	var minV, maxV interface{}
	if err := db.QueryRow(q).Scan(&minV, &maxV); err != nil {
		return nil, nil, err
	}
	return minV, maxV, nil
}

func PrintCompareSummary(results []CompareResult) {
	var ok, warn, errCount int
	for _, r := range results {
		if len(r.Errors) > 0 {
			errCount++
		} else if r.CountMatch && r.MismatchedRows == 0 && r.SourceOnlyRows == 0 && r.TargetOnlyRows == 0 {
			ok++
		} else {
			warn++
		}
	}

	logger.Info("\nCompare summary: OK=%d WARN=%d ERROR=%d", ok, warn, errCount)

	for _, r := range results {
		status := "OK"
		if len(r.Errors) > 0 {
			status = "ERROR"
		} else if !r.CountMatch || r.MismatchedRows > 0 || r.SourceOnlyRows > 0 || r.TargetOnlyRows > 0 {
			status = "WARN"
		}
		logger.Info("Table: %s -> %s [%s] Count: %d vs %d", r.SourceTable, r.TargetTable, status, r.SourceCount, r.TargetCount)
		if len(r.SampleMismatches) > 0 {
			for _, m := range r.SampleMismatches {
				logger.Warn("  Mismatch key(%s): %s diff_cols=%v", m.KeyType, m.Key, m.DiffCols)
			}
		}
		if r.NoPKUK && len(r.ColumnStats) > 0 {
			for _, cs := range r.ColumnStats {
				if !cs.MinMatch || !cs.MaxMatch {
					logger.Warn("  Column %s MinMatch=%v MaxMatch=%v srcMin=%v tgtMin=%v srcMax=%v tgtMax=%v",
						cs.ColumnName, cs.MinMatch, cs.MaxMatch, cs.SourceMin, cs.TargetMin, cs.SourceMax, cs.TargetMax)
				}
			}
		}
		if len(r.Errors) > 0 {
			for _, e := range r.Errors {
				logger.Error("  Error: %s", e)
			}
		}
	}
}

package compare

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/huayumicheng/MySQL2OBSync/internal/database"
	"github.com/huayumicheng/MySQL2OBSync/internal/logger"
)

type Comparator struct {
	sourceDB *database.Connection
	targetDB *database.Connection
	workers  int

	drillThreshold int
	checkpoints    *checkpointManager
}

func NewComparator(sourceDB, targetDB *database.Connection, workers int, checkpointFile string, drillThreshold int) *Comparator {
	if workers <= 0 {
		workers = 10
	}
	if drillThreshold <= 0 {
		drillThreshold = 2000
	}
	cp, err := newCheckpointManager(checkpointFile)
	if err != nil {
		logger.Warn("Failed to init compare checkpoint: %v", err)
	}
	return &Comparator{sourceDB: sourceDB, targetDB: targetDB, workers: workers, drillThreshold: drillThreshold, checkpoints: cp}
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
	SourceTable    string
	TargetTable    string
	SourceCount    int64
	TargetCount    int64
	CountMatch     bool
	KeyColumn      string
	KeyType        string
	ComparedKeys   int64
	MatchedKeys    int64
	MismatchedKeys int64
	SourceOnlyKeys int64
	TargetOnlyKeys int64
	Errors         []string
}

type TablePair struct {
	Source      string
	Target      string
	SplitColumn string
	CountOnly   bool
	BatchKeys   int
}

func (c *Comparator) CompareTables(tables []TablePair) ([]CompareResult, error) {
	if c.checkpoints != nil {
		h, err := hashConfig(struct {
			DrillThreshold int         `json:"drill_threshold"`
			Tables         []TablePair `json:"tables"`
		}{DrillThreshold: c.drillThreshold, Tables: tables})
		if err == nil {
			if err := c.checkpoints.init(h); err != nil {
				logger.Warn("Failed to init compare checkpoint: %v", err)
			}
		} else {
			logger.Warn("Failed to hash compare config: %v", err)
		}
	}

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

			result, err := c.CompareTable(t)
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

func (c *Comparator) CompareTable(t TablePair) (CompareResult, error) {
	result := CompareResult{SourceTable: t.Source, TargetTable: t.Target}

	logger.Info("\nComparing table: %s <-> %s", t.Source, t.Target)

	sourceCount, targetCount, countMatch, err := c.compareRowCount(t.Source, t.Target)
	if err != nil {
		return result, err
	}
	result.SourceCount = sourceCount
	result.TargetCount = targetCount
	result.CountMatch = countMatch

	logger.Info("  [%s] Source count: %d, Target count: %d, Match: %v", t.Source, sourceCount, targetCount, countMatch)
	if !countMatch {
		logger.Warn("  WARNING: Row count mismatch!")
	}
	if t.CountOnly {
		logger.Info("  [%s] Count-only mode, skipping data content comparison", t.Source)
		return result, nil
	}

	if sourceCount == 0 && targetCount == 0 {
		return result, nil
	}

	if err := c.chunkCompareByKeyColumn(&result, t); err != nil {
		return result, err
	}
	return result, nil
}

type aggStat struct {
	cnt    int64
	sumCrc int64
	xorCrc int64
}

func (c *Comparator) chunkCompareByKeyColumn(result *CompareResult, t TablePair) error {
	keyCols, keyType, err := c.getUniqueKey(t.Source)
	keyCol := ""
	if err == nil && len(keyCols) == 1 {
		keyCol = keyCols[0]
	}
	if keyCol == "" {
		if strings.TrimSpace(t.SplitColumn) == "" {
			return fmt.Errorf("no PK/UK (single column) found and split_column is empty: table=%s", t.Source)
		}
		keyCol = t.SplitColumn
		keyType = "SPLIT_COLUMN"
	}

	if t.BatchKeys <= 0 {
		t.BatchKeys = 1000
	}

	srcCols, err := c.getTableColumns(c.sourceDB.DB, t.Source)
	if err != nil {
		return err
	}
	tgtCols, err := c.getTableColumns(c.targetDB.DB, t.Target)
	if err != nil {
		return err
	}
	if !sameStringSliceCI(srcCols, tgtCols) {
		return fmt.Errorf("column list mismatch between source and target: %s <-> %s", t.Source, t.Target)
	}

	rowExpr := buildRowExpr(srcCols)

	result.KeyColumn = keyCol
	result.KeyType = keyType

	distinctKeys := keyType == "SPLIT_COLUMN"
	colType, err := c.getColumnDataType(c.sourceDB.DB, t.Source, keyCol)
	if err != nil {
		return err
	}

	var last interface{} = nil
	if c.checkpoints != nil {
		if ck, ok := c.checkpoints.get(t.Source); ok && ck.Done && ck.KeyColumn == keyCol && ck.KeyType == keyType {
			logger.Info("  [%s] Compare checkpoint done, skipping content comparison", t.Source)
			return nil
		}
		if ck, ok := c.checkpoints.get(t.Source); ok && ck.LastKey != "" && ck.KeyColumn == keyCol && ck.KeyType == keyType {
			v, err := parseCheckpointKey(ck.LastKey, colType)
			if err == nil {
				last = v
				logger.Info("  [%s] Compare checkpoint resume from last_key=%s", t.Source, ck.LastKey)
			}
		}
	}

	for {
		keys, err := c.getNextKeys(t.Source, keyCol, last, t.BatchKeys, distinctKeys)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			break
		}
		endKey := keys[len(keys)-1]

		if err := c.compareChunkAndDrill(result, t, keyCol, keyType, rowExpr, last, endKey, keys, distinctKeys); err != nil {
			return err
		}
		last = endKey
		if c.checkpoints != nil {
			lastKeyStr := ""
			if last != nil {
				lastKeyStr = fmt.Sprint(normalizeValue(last))
			}
			_ = c.checkpoints.set(t.Source, tableCheckpoint{
				LastKey:   lastKeyStr,
				Done:      false,
				KeyColumn: keyCol,
				KeyType:   keyType,
			})
		}
	}

	logger.Info("  [%s] Key=%s(%s) ComparedKeys=%d Matched=%d Mismatched=%d SourceOnly=%d TargetOnly=%d",
		t.Source, keyCol, keyType, result.ComparedKeys, result.MatchedKeys, result.MismatchedKeys, result.SourceOnlyKeys, result.TargetOnlyKeys)

	if c.checkpoints != nil {
		lastKeyStr := ""
		if last != nil {
			lastKeyStr = fmt.Sprint(normalizeValue(last))
		}
		_ = c.checkpoints.set(t.Source, tableCheckpoint{
			LastKey:   lastKeyStr,
			Done:      true,
			KeyColumn: keyCol,
			KeyType:   keyType,
		})
	}

	return nil
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

func parseCheckpointKey(s string, colType string) (interface{}, error) {
	ct := strings.ToLower(strings.TrimSpace(colType))
	switch ct {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year":
		v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "unsigned":
		v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
		if err != nil {
			return nil, err
		}
		return int64(v), nil
	case "datetime", "timestamp", "date", "time":
		layout := "2006-01-02 15:04:05.000000"
		t, err := time.ParseInLocation(layout, strings.TrimSpace(s), time.UTC)
		if err != nil {
			return nil, err
		}
		return t, nil
	default:
		return s, nil
	}
}

func (c *Comparator) getColumnDataType(db *sql.DB, table string, col string) (string, error) {
	q := `
		SELECT data_type
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		  AND column_name = ?
	`
	var dt string
	if err := db.QueryRow(q, table, col).Scan(&dt); err != nil {
		return "", err
	}
	return dt, nil
}

func (c *Comparator) getNextKeys(table string, keyCol string, last interface{}, limit int, distinct bool) ([]interface{}, error) {
	if limit <= 0 {
		limit = 1000
	}
	qc := database.QuoteIdent(keyCol)
	qt := database.QuoteTable(table)
	var (
		query string
		args  []interface{}
	)
	if last == nil {
		if distinct {
			query = fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s LIMIT ?", qc, qt, qc, qc)
		} else {
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s LIMIT ?", qc, qt, qc, qc)
		}
		args = []interface{}{limit}
	} else {
		if distinct {
			query = fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL AND %s > ? ORDER BY %s LIMIT ?", qc, qt, qc, qc, qc)
		} else {
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL AND %s > ? ORDER BY %s LIMIT ?", qc, qt, qc, qc, qc)
		}
		args = []interface{}{last, limit}
	}

	rows, err := c.sourceDB.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]interface{}, 0, limit)
	for rows.Next() {
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Comparator) getKeysInRange(db *sql.DB, table string, keyCol string, startExclusive interface{}, endInclusive interface{}, distinct bool) ([]interface{}, error) {
	qc := database.QuoteIdent(keyCol)
	qt := database.QuoteTable(table)

	var (
		query string
		args  []interface{}
	)
	if startExclusive == nil {
		if distinct {
			query = fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL AND %s <= ? ORDER BY %s", qc, qt, qc, qc, qc)
		} else {
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL AND %s <= ? ORDER BY %s", qc, qt, qc, qc, qc)
		}
		args = []interface{}{endInclusive}
	} else {
		if distinct {
			query = fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL AND %s > ? AND %s <= ? ORDER BY %s", qc, qt, qc, qc, qc, qc)
		} else {
			query = fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL AND %s > ? AND %s <= ? ORDER BY %s", qc, qt, qc, qc, qc, qc)
		}
		args = []interface{}{startExclusive, endInclusive}
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]interface{}, 0)
	for rows.Next() {
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Comparator) getRangeAgg(db *sql.DB, table string, keyCol string, rowExpr string, startExclusive interface{}, endInclusive interface{}) (aggStat, error) {
	qc := database.QuoteIdent(keyCol)
	qt := database.QuoteTable(table)

	var (
		query string
		args  []interface{}
	)
	if startExclusive == nil {
		query = fmt.Sprintf("SELECT COUNT(*) AS cnt, COALESCE(SUM(CRC32(%s)), 0) AS sum_crc, COALESCE(BIT_XOR(CRC32(%s)), 0) AS xor_crc FROM %s WHERE %s IS NOT NULL AND %s <= ?",
			rowExpr, rowExpr, qt, qc, qc)
		args = []interface{}{endInclusive}
	} else {
		query = fmt.Sprintf("SELECT COUNT(*) AS cnt, COALESCE(SUM(CRC32(%s)), 0) AS sum_crc, COALESCE(BIT_XOR(CRC32(%s)), 0) AS xor_crc FROM %s WHERE %s IS NOT NULL AND %s > ? AND %s <= ?",
			rowExpr, rowExpr, qt, qc, qc, qc)
		args = []interface{}{startExclusive, endInclusive}
	}

	var (
		cnt    int64
		sumCrc int64
		xorCrc int64
	)
	if err := db.QueryRow(query, args...).Scan(&cnt, &sumCrc, &xorCrc); err != nil {
		return aggStat{}, err
	}
	return aggStat{cnt: cnt, sumCrc: sumCrc, xorCrc: xorCrc}, nil
}

func (c *Comparator) compareChunkAndDrill(result *CompareResult, t TablePair, keyCol string, keyType string, rowExpr string, startExclusive interface{}, endInclusive interface{}, keys []interface{}, distinct bool) error {
	srcAgg, err := c.getRangeAgg(c.sourceDB.DB, t.Source, keyCol, rowExpr, startExclusive, endInclusive)
	if err != nil {
		return err
	}
	tgtAgg, err := c.getRangeAgg(c.targetDB.DB, t.Target, keyCol, rowExpr, startExclusive, endInclusive)
	if err != nil {
		return err
	}
	if srcAgg.cnt == tgtAgg.cnt && srcAgg.sumCrc == tgtAgg.sumCrc && srcAgg.xorCrc == tgtAgg.xorCrc {
		result.ComparedKeys += int64(len(keys))
		result.MatchedKeys += int64(len(keys))
		return nil
	}
	return c.drillRange(result, t, keyCol, keyType, rowExpr, startExclusive, endInclusive, keys, distinct)
}

func (c *Comparator) drillRange(result *CompareResult, t TablePair, keyCol string, keyType string, rowExpr string, startExclusive interface{}, endInclusive interface{}, keys []interface{}, distinct bool) error {
	if len(keys) <= c.drillThreshold {
		srcKeys := keys
		tgtKeys, err := c.getKeysInRange(c.targetDB.DB, t.Target, keyCol, startExclusive, endInclusive, distinct)
		if err != nil {
			return err
		}
		keyMap := make(map[string]interface{}, len(srcKeys)+len(tgtKeys))
		for _, k := range srcKeys {
			keyMap[fmt.Sprint(normalizeValue(k))] = k
		}
		for _, k := range tgtKeys {
			ks := fmt.Sprint(normalizeValue(k))
			if _, ok := keyMap[ks]; !ok {
				keyMap[ks] = k
			}
		}
		union := make([]interface{}, 0, len(keyMap))
		for _, v := range keyMap {
			union = append(union, v)
		}

		srcAggByKey, err := c.getAggByKeys(c.sourceDB.DB, t.Source, keyCol, rowExpr, union)
		if err != nil {
			return err
		}
		tgtAggByKey, err := c.getAggByKeys(c.targetDB.DB, t.Target, keyCol, rowExpr, union)
		if err != nil {
			return err
		}

		for ks := range keyMap {
			sa, sok := srcAggByKey[ks]
			ta, tok := tgtAggByKey[ks]
			result.ComparedKeys++
			if !sok && tok {
				result.TargetOnlyKeys++
				continue
			}
			if sok && !tok {
				result.SourceOnlyKeys++
				continue
			}
			if !sok && !tok {
				continue
			}
			if sa.cnt == ta.cnt && sa.sumCrc == ta.sumCrc && sa.xorCrc == ta.xorCrc {
				result.MatchedKeys++
			} else {
				result.MismatchedKeys++
			}
		}
		return nil
	}

	mid := len(keys) / 2
	if mid <= 0 {
		return nil
	}
	leftKeys := keys[:mid]
	rightKeys := keys[mid:]

	pivot := leftKeys[len(leftKeys)-1]

	if err := c.compareChunkAndDrill(result, t, keyCol, keyType, rowExpr, startExclusive, pivot, leftKeys, distinct); err != nil {
		return err
	}
	return c.compareChunkAndDrill(result, t, keyCol, keyType, rowExpr, pivot, endInclusive, rightKeys, distinct)
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

func buildRowExpr(cols []string) string {
	parts := make([]string, 0, len(cols))
	for _, col := range cols {
		qc := database.QuoteIdent(col)
		parts = append(parts, fmt.Sprintf("IFNULL(CAST(%s AS CHAR), 'NULL')", qc))
	}
	return fmt.Sprintf("CONCAT_WS('#', %s)", strings.Join(parts, ", "))
}

func sameStringSliceCI(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	na := make([]string, len(a))
	nb := make([]string, len(b))
	for i := range a {
		na[i] = strings.ToLower(strings.TrimSpace(a[i]))
	}
	for i := range b {
		nb[i] = strings.ToLower(strings.TrimSpace(b[i]))
	}
	sort.Strings(na)
	sort.Strings(nb)
	for i := range na {
		if na[i] != nb[i] {
			return false
		}
	}
	return true
}

func (c *Comparator) getDistinctKeys(table string, keyCol string, last interface{}, limit int) ([]interface{}, error) {
	if limit <= 0 {
		limit = 1000
	}

	qc := database.QuoteIdent(keyCol)
	qt := database.QuoteTable(table)

	var (
		query string
		args  []interface{}
	)
	if last == nil {
		query = fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL ORDER BY %s LIMIT ?", qc, qt, qc, qc)
		args = []interface{}{limit}
	} else {
		query = fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL AND %s > ? ORDER BY %s LIMIT ?", qc, qt, qc, qc, qc)
		args = []interface{}{last, limit}
	}

	rows, err := c.sourceDB.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]interface{}, 0, limit)
	for rows.Next() {
		var v interface{}
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Comparator) getAggByKeys(db *sql.DB, table string, keyCol string, rowExpr string, keys []interface{}) (map[string]aggStat, error) {
	out := make(map[string]aggStat, len(keys))
	if len(keys) == 0 {
		return out, nil
	}

	qc := database.QuoteIdent(keyCol)
	qt := database.QuoteTable(table)

	ph := make([]string, 0, len(keys))
	args := make([]interface{}, 0, len(keys))
	for _, k := range keys {
		ph = append(ph, "?")
		args = append(args, k)
	}

	query := fmt.Sprintf(
		"SELECT %s, COUNT(*) AS cnt, COALESCE(SUM(CRC32(%s)), 0) AS sum_crc, COALESCE(BIT_XOR(CRC32(%s)), 0) AS xor_crc FROM %s WHERE %s IN (%s) GROUP BY %s",
		qc,
		rowExpr,
		rowExpr,
		qt,
		qc,
		strings.Join(ph, ","),
		qc,
	)

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			k      interface{}
			cnt    int64
			sumCrc int64
			xorCrc int64
		)
		if err := rows.Scan(&k, &cnt, &sumCrc, &xorCrc); err != nil {
			return nil, err
		}
		ks := fmt.Sprint(normalizeValue(k))
		out[ks] = aggStat{cnt: cnt, sumCrc: sumCrc, xorCrc: xorCrc}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func PrintCompareSummary(results []CompareResult) {
	var ok, warn, errCount int
	for _, r := range results {
		if len(r.Errors) > 0 {
			errCount++
		} else if r.CountMatch && r.MismatchedKeys == 0 && r.SourceOnlyKeys == 0 && r.TargetOnlyKeys == 0 {
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
		} else if !r.CountMatch || r.MismatchedKeys > 0 || r.SourceOnlyKeys > 0 || r.TargetOnlyKeys > 0 {
			status = "WARN"
		}
		logger.Info("Table: %s -> %s [%s] Count: %d vs %d Key: %s(%s) ComparedKeys=%d Mismatched=%d",
			r.SourceTable, r.TargetTable, status, r.SourceCount, r.TargetCount, r.KeyColumn, r.KeyType, r.ComparedKeys, r.MismatchedKeys)
		if len(r.Errors) > 0 {
			for _, e := range r.Errors {
				logger.Error("  Error: %s", e)
			}
		}
	}
}

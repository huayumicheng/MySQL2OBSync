package sync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huayumicheng/MySQL2OBSync/internal/config"
	"github.com/huayumicheng/MySQL2OBSync/internal/database"
	"github.com/huayumicheng/MySQL2OBSync/internal/logger"
	"github.com/huayumicheng/MySQL2OBSync/internal/monitor"
	"github.com/huayumicheng/MySQL2OBSync/internal/schema"
)

type Engine struct {
	config   *config.Config
	sourceDB *database.Connection
	targetDB *database.Connection
	stats    *monitor.SyncStats
	ctx      context.Context
	cancel   context.CancelFunc

	allowNonEmptyTargets map[string]struct{}
}

func NewEngine(cfg *config.Config, sourceDB, targetDB *database.Connection) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	return &Engine{
		config:   cfg,
		sourceDB: sourceDB,
		targetDB: targetDB,
		stats:    monitor.NewSyncStats(),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (e *Engine) Stop()                        { e.cancel() }
func (e *Engine) GetStats() *monitor.SyncStats { return e.stats }

type TargetTableNotEmptyError struct {
	Table  string
	Count  int64
	Advice string
}

func (e *TargetTableNotEmptyError) Error() string {
	if e.Advice != "" {
		return fmt.Sprintf("target table not empty: %s rows=%d (%s)", e.Table, e.Count, e.Advice)
	}
	return fmt.Sprintf("target table not empty: %s rows=%d", e.Table, e.Count)
}

type TargetTablesNotEmptyError struct {
	Tables []TargetTableNotEmptyError
}

func (e *TargetTablesNotEmptyError) Error() string {
	parts := make([]string, 0, len(e.Tables))
	for _, t := range e.Tables {
		parts = append(parts, fmt.Sprintf("%s=%d", t.Table, t.Count))
	}
	return fmt.Sprintf("target has existing data, aborting sync: %s", strings.Join(parts, ", "))
}

type TableSyncFailure struct {
	Table string
	Err   string
}

type SyncFailedTablesError struct {
	Failures []TableSyncFailure
}

func (e *SyncFailedTablesError) Error() string {
	if len(e.Failures) == 0 {
		return "sync failed: unknown error"
	}
	parts := make([]string, 0, len(e.Failures))
	for _, f := range e.Failures {
		parts = append(parts, fmt.Sprintf("%s: %s", f.Table, f.Err))
	}
	return fmt.Sprintf("sync failed: %d table(s) failed: %s", len(e.Failures), strings.Join(parts, "; "))
}

func isTargetNotEmptyError(err error) bool {
	var one *TargetTableNotEmptyError
	if errors.As(err, &one) {
		return true
	}
	var many *TargetTablesNotEmptyError
	return errors.As(err, &many)
}

func isInteractiveStdin() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

type targetTableCount struct {
	Table              string
	Rows               int64
	TruncateBeforeSync bool
}

func (e *Engine) precheckTargetTablesAndSelect(tables []config.TableConfig) ([]config.TableConfig, error) {
	empty := make([]targetTableCount, 0, len(tables))
	nonEmpty := make([]targetTableCount, 0, len(tables))

	countByTarget := make(map[string]int64, len(tables))

	for _, t := range tables {
		cnt, err := database.GetTableRowCount(e.targetDB.DB, t.Target)
		if err != nil {
			return nil, fmt.Errorf("precheck target row count failed for %s: %w", t.Target, err)
		}
		countByTarget[t.Target] = cnt
		if cnt == 0 {
			empty = append(empty, targetTableCount{Table: t.Target, Rows: cnt, TruncateBeforeSync: t.TruncateBeforeSync})
		} else {
			nonEmpty = append(nonEmpty, targetTableCount{Table: t.Target, Rows: cnt, TruncateBeforeSync: t.TruncateBeforeSync})
		}
	}

	logger.Info("\nTarget table row count precheck:")
	if len(empty) > 0 {
		logger.Info("  Empty tables:")
		for _, s := range empty {
			logger.Info("    - %s = %d", s.Table, s.Rows)
		}
	} else {
		logger.Info("  Empty tables: (none)")
	}
	if len(nonEmpty) > 0 {
		logger.Info("  Non-empty tables:")
		for _, s := range nonEmpty {
			mark := "truncate_before_sync=false"
			if s.TruncateBeforeSync {
				mark = "truncate_before_sync=true"
			}
			logger.Info("    - %s = %d (%s)", s.Table, s.Rows, mark)
		}
	} else {
		logger.Info("  Non-empty tables: (none)")
	}

	if len(nonEmpty) == 0 {
		return tables, nil
	}

	if !isInteractiveStdin() {
		return nil, fmt.Errorf("target has existing data, aborting sync (non-interactive mode)")
	}

	fmt.Fprintln(os.Stdout, "")
	fmt.Fprintln(os.Stdout, "Target has existing data in some tables.")
	fmt.Fprintln(os.Stdout, "Input NO to abort.")
	fmt.Fprintln(os.Stdout, "Input YES_EMPTY to sync ONLY tables whose target row count is 0.")
	fmt.Fprint(os.Stdout, "Your choice (YES_EMPTY/NO): ")

	var choice string
	if _, err := fmt.Scanln(&choice); err != nil {
		return nil, fmt.Errorf("read choice failed: %w", err)
	}
	choice = strings.TrimSpace(strings.ToLower(choice))
	if choice == "no" {
		return nil, fmt.Errorf("sync aborted by user")
	}
	if choice != "yes_empty" {
		return nil, fmt.Errorf("invalid choice: %s", choice)
	}

	selected := make([]config.TableConfig, 0, len(empty))
	for _, t := range tables {
		if countByTarget[t.Target] == 0 {
			selected = append(selected, t)
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("no empty target tables to sync")
	}

	logger.Info("\nWill sync EMPTY target tables (%d):", len(selected))
	for _, t := range selected {
		logger.Info("  - %s -> %s", t.Source, t.Target)
	}

	e.allowNonEmptyTargets = nil

	return selected, nil
}

func (e *Engine) Run() error {
	tables := e.config.Tables
	if len(tables) == 0 {
		all, err := schema.NewDiscovery(e.sourceDB.DB).DiscoverAllTables()
		if err != nil {
			return fmt.Errorf("discover tables failed: %w", err)
		}
		for _, t := range all {
			tables = append(tables, config.TableConfig{
				Source:             t,
				Target:             t,
				TruncateBeforeSync: true,
			})
		}
	}

	selected, err := e.precheckTargetTablesAndSelect(tables)
	if err != nil {
		return err
	}

	e.stats.Start()
	if e.config.Monitor.ReportInterval != "" {
		go e.stats.StartReporting(e.config.GetReportInterval())
	}
	e.stats.TotalTables = len(selected)

	if e.config.Sync.TableWorkers <= 1 {
		return e.runSequential(selected)
	}
	return e.runConcurrent(selected, e.config.Sync.TableWorkers)
}

func (e *Engine) runSequential(tables []config.TableConfig) error {
	var failures []TableSyncFailure
	for i, table := range tables {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		default:
		}

		if err := e.syncTable(table, i+1, len(tables)); err != nil {
			logger.Error("  [%s] Table sync failed: %v", table.Source, err)
			e.stats.RecordFailedTable(table.Source, err.Error())
			if isTargetNotEmptyError(err) {
				e.Stop()
				e.stats.Stop()
				return err
			}
			failures = append(failures, TableSyncFailure{Table: table.Source, Err: err.Error()})
		}
	}
	e.stats.Stop()
	if len(failures) > 0 {
		return &SyncFailedTablesError{Failures: failures}
	}
	return nil
}

func (e *Engine) runConcurrent(tables []config.TableConfig, tableWorkers int) error {
	logger.Info("\nStarting concurrent sync with %d table workers", tableWorkers)

	logger.Info("Calculating total rows for all tables...")
	var totalRowsAll int64
	for _, table := range tables {
		rows, err := database.GetTableRowCount(e.sourceDB.DB, table.Source)
		if err != nil {
			logger.Warn("Failed to get row count for %s: %v", table.Source, err)
			continue
		}
		totalRowsAll += rows
	}
	atomic.StoreInt64(&e.stats.TotalRows, totalRowsAll)
	logger.Info("Total rows to sync: %s", monitor.FormatNumber(totalRowsAll))

	var wg sync.WaitGroup
	tableChan := make(chan config.TableConfig, len(tables))
	tableIndex := int32(0)
	totalTables := len(tables)
	var fatalOnce sync.Once
	var fatalErr error
	var failedMu sync.Mutex
	var failures []TableSyncFailure

	for i := 0; i < tableWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("Table worker %d panic: %v", workerID, r)
				}
				wg.Done()
			}()
			for table := range tableChan {
				select {
				case <-e.ctx.Done():
					return
				default:
				}

				idx := atomic.AddInt32(&tableIndex, 1)
				logger.Info("[%d/%d] Syncing table: %s -> %s", idx, totalTables, table.Source, table.Target)

				if err := e.syncTable(table, int(idx), totalTables); err != nil {
					logger.Error("  [%s] Table sync failed: %v", table.Source, err)
					e.stats.RecordFailedTable(table.Source, err.Error())
					failedMu.Lock()
					failures = append(failures, TableSyncFailure{Table: table.Source, Err: err.Error()})
					failedMu.Unlock()
					if isTargetNotEmptyError(err) {
						fatalOnce.Do(func() {
							fatalErr = err
							e.Stop()
						})
						return
					}
				} else {
					completed := atomic.LoadInt64(&e.stats.CompletedTables)
					logger.Info("✓ Table %s completed (%d/%d)", table.Source, completed, totalTables)
				}
			}
		}(i)
	}

	go func() {
		for _, table := range tables {
			tableChan <- table
		}
		close(tableChan)
	}()

	wg.Wait()
	e.stats.Stop()
	if fatalErr != nil {
		return fatalErr
	}
	if len(failures) > 0 {
		return &SyncFailedTablesError{Failures: failures}
	}
	return nil
}

func (e *Engine) syncTable(tableConfig config.TableConfig, tableIndex int, totalTables int) error {
	ts, err := schema.NewDiscovery(e.sourceDB.DB).DiscoverTable(tableConfig.Source)
	if err != nil {
		return fmt.Errorf("discover table schema failed: %w", err)
	}

	totalRows, err := database.GetTableRowCount(e.sourceDB.DB, tableConfig.Source)
	if err != nil {
		return fmt.Errorf("get row count failed: %w", err)
	}

	tableStats := e.stats.StartTable(tableConfig.Source, totalRows)

	pkInfo := ts.PrimaryKey
	if len(ts.PrimaryKeys) > 1 {
		pkInfo = fmt.Sprintf("%s (composite: %s)", ts.PrimaryKey, strings.Join(ts.PrimaryKeys, ", "))
	}
	logger.Info("[%d/%d] [%s] Estimated rows: %d, Primary key: %s", tableIndex, totalTables, tableConfig.Source, totalRows, pkInfo)

	e.checkLargeColumns(tableConfig.Source, ts)

	if tableConfig.TruncateBeforeSync {
		if err := database.TruncateTable(e.targetDB.DB, tableConfig.Target); err != nil {
			return fmt.Errorf("truncate target table failed: %w", err)
		}
		logger.Info("[%s] Target table truncated", tableConfig.Source)
	} else {
		cnt, err := database.GetTableRowCount(e.targetDB.DB, tableConfig.Target)
		if err != nil {
			return fmt.Errorf("get target row count failed: %w", err)
		}
		if cnt > 0 {
			if e.allowNonEmptyTargets != nil {
				if _, ok := e.allowNonEmptyTargets[tableConfig.Target]; ok {
					logger.Warn("[%s] Target table not empty (%s=%d), continuing due to precheck confirmation", tableConfig.Source, tableConfig.Target, cnt)
				} else {
					return &TargetTableNotEmptyError{
						Table:  tableConfig.Target,
						Count:  cnt,
						Advice: "set truncate_before_sync: true to allow truncation or clean target data",
					}
				}
			} else {
				return &TargetTableNotEmptyError{
					Table:  tableConfig.Target,
					Count:  cnt,
					Advice: "set truncate_before_sync: true to allow truncation or clean target data",
				}
			}
		}
	}

	splitColumn := tableConfig.SplitColumn
	if splitColumn == "" {
		splitColumn = ts.PrimaryKey
	}

	var syncErr error
	if len(ts.PrimaryKeys) > 1 {
		logger.Info("  [%s] Composite primary key detected (%v), using sequential mode", tableConfig.Source, ts.PrimaryKeys)
		syncErr = e.syncTableSequential(tableConfig, ts, tableStats)
	} else if splitColumn != "" && totalRows > int64(e.config.Sync.BatchSize) {
		minID, maxID, ok, err := database.GetPrimaryKeyRange(e.sourceDB.DB, tableConfig.Source, splitColumn)
		if err == nil && ok && minID > 0 && maxID > 0 {
			keyRange := maxID - minID + 1
			if keyRange/int64(totalRows) > 100 {
				logger.Info("  [%s] Data distribution uneven (range/rows=%d), using sequential mode", tableConfig.Source, keyRange/int64(totalRows))
				syncErr = e.syncTableSequential(tableConfig, ts, tableStats)
			} else {
				syncErr = e.syncTableWithSplit(tableConfig, ts, splitColumn, totalRows, minID, maxID, tableStats)
			}
		} else {
			syncErr = e.syncTableSequential(tableConfig, ts, tableStats)
		}
	} else {
		syncErr = e.syncTableSequential(tableConfig, ts, tableStats)
	}

	if syncErr == nil {
		e.stats.CompleteTable(tableConfig.Source)
	}
	return syncErr
}

type SplitTask struct {
	Start int64
	End   int64
}

func (e *Engine) createSplitTasks(min, max int64, workerCount int) []SplitTask {
	if workerCount <= 0 {
		workerCount = 1
	}
	total := max - min + 1
	perWorker := total / int64(workerCount)
	if perWorker == 0 {
		perWorker = 1
	}
	tasks := make([]SplitTask, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		start := min + int64(i)*perWorker
		end := start + perWorker - 1
		if i == workerCount-1 {
			end = max
		}
		if start > max {
			break
		}
		tasks = append(tasks, SplitTask{Start: start, End: end})
	}
	return tasks
}

func (e *Engine) syncTableWithSplit(tableConfig config.TableConfig, ts *schema.TableSchema, splitColumn string, totalRows int64, minID, maxID int64, tableStats *monitor.TableStats) error {
	channelBuffer := e.config.Sync.ChannelBuffer
	if channelBuffer <= 0 {
		channelBuffer = 10000
	}
	logger.Info("  [%s] Using channel buffer size: %d (workers: %d)", tableConfig.Source, channelBuffer, e.config.Sync.Workers)

	columns := make([]string, 0, len(ts.Columns))
	for _, col := range ts.Columns {
		columns = append(columns, col.Name)
	}

	dataChan := make(chan [][]interface{}, channelBuffer)

	var wg sync.WaitGroup
	writerErrors := make(chan error, e.config.Sync.Workers)
	writerCount := e.config.Sync.Workers / 2
	if writerCount < 1 {
		writerCount = 1
	}
	for i := 0; i < writerCount; i++ {
		wg.Add(1)
		go e.tableWriter(tableConfig, columns, dataChan, &wg, writerErrors, tableStats)
	}

	tasks := e.createSplitTasks(minID, maxID, e.config.Sync.Workers)
	readWg := sync.WaitGroup{}
	readerErrors := make(chan error, len(tasks))

	var failedTasks []SplitTask
	var failedMu sync.Mutex

	for _, task := range tasks {
		readWg.Add(1)
		go func(t SplitTask) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("  [%s] Reader panic: %v", tableConfig.Source, r)
				}
				readWg.Done()
			}()
			if err := e.tableReader(tableConfig, columns, splitColumn, t, dataChan); err != nil {
				logger.Error("  [%s] Reader error: %v", tableConfig.Source, err)
				failedMu.Lock()
				failedTasks = append(failedTasks, t)
				failedMu.Unlock()
				readerErrors <- err
			}
		}(task)
	}

	go func() {
		readWg.Wait()
		close(dataChan)
		close(readerErrors)
	}()

	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-time.After(30 * time.Minute):
			logger.Error("  [%s] Table sync timeout (30min), forcing close", tableConfig.Source)
			close(dataChan)
		}
	}()

	wg.Wait()
	close(done)
	close(writerErrors)

	var readerErr error
	for err := range readerErrors {
		if err != nil && readerErr == nil {
			readerErr = err
		}
	}

	var writerErr error
	for err := range writerErrors {
		if err != nil && writerErr == nil {
			writerErr = err
			logger.Error("  [%s] Writer error detected: %v", tableConfig.Source, err)
		}
	}

	if len(failedTasks) > 0 {
		logger.Warn("  [%s] %d range(s) failed in parallel mode, compensating with sequential mode", tableConfig.Source, len(failedTasks))
		for _, task := range failedTasks {
			logger.Info("  [%s] Compensating range %d-%d", tableConfig.Source, task.Start, task.End)
			if err := e.tableReaderSequential(tableConfig, columns, splitColumn, task, tableStats); err != nil {
				logger.Error("  [%s] Compensation failed for range %d-%d: %v", tableConfig.Source, task.Start, task.End, err)
				return fmt.Errorf("reader error (compensation failed): %w", err)
			}
		}
	}

	if writerErr != nil {
		return fmt.Errorf("writer error: %w", writerErr)
	}

	if len(failedTasks) > 0 {
		readerErr = nil
	}
	if readerErr != nil {
		return fmt.Errorf("reader error: %w", readerErr)
	}
	return nil
}

func (e *Engine) syncTableSequential(tableConfig config.TableConfig, ts *schema.TableSchema, tableStats *monitor.TableStats) error {
	columns := make([]string, 0, len(ts.Columns))
	quotedColumns := make([]string, 0, len(ts.Columns))
	for _, col := range ts.Columns {
		columns = append(columns, col.Name)
		quotedColumns = append(quotedColumns, database.QuoteIdent(col.Name))
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quotedColumns, ", "), database.QuoteTable(tableConfig.Source))
	if tableConfig.Where != "" {
		query += " WHERE " + tableConfig.Where
	}

	rows, err := e.sourceDB.DB.QueryContext(e.ctx, query)
	if err != nil {
		return fmt.Errorf("query source table failed: %w", err)
	}
	defer rows.Close()

	batch := make([][]interface{}, 0, e.config.Sync.BatchSize)

	for rows.Next() {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		default:
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		batch = append(batch, values)

		if len(batch) >= e.config.Sync.BatchSize {
			if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
				logger.Error("  [%s] Insert batch failed: %v", tableConfig.Source, err)
				return err
			}
			e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
			batch = nil
			batch = make([][]interface{}, 0, e.config.Sync.BatchSize)
		}
	}

	if len(batch) > 0 {
		if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
			logger.Error("  [%s] Insert final batch failed: %v", tableConfig.Source, err)
			return err
		}
		e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
	}

	return rows.Err()
}

func (e *Engine) tableReader(tableConfig config.TableConfig, columns []string, splitColumn string, task SplitTask, dataChan chan<- [][]interface{}) error {
	conn, err := e.sourceDB.DB.Conn(e.ctx)
	if err != nil {
		return fmt.Errorf("get dedicated connection failed: %w", err)
	}
	defer conn.Close()

	readerDone := make(chan error, 1)
	go func() {
		readerDone <- e.tableReaderWithConn(conn, tableConfig, columns, splitColumn, task, dataChan)
	}()

	select {
	case err := <-readerDone:
		return err
	case <-time.After(15 * time.Minute):
		logger.Error("  [%s] Reader timeout (15min) for range %d-%d, closing connection", tableConfig.Source, task.Start, task.End)
		_ = conn.Close()
		return fmt.Errorf("tableReader timeout (15min) for range %d-%d", task.Start, task.End)
	}
}

func (e *Engine) tableReaderWithConn(conn *sql.Conn, tableConfig config.TableConfig, columns []string, splitColumn string, task SplitTask, dataChan chan<- [][]interface{}) error {
	quotedCols := make([]string, 0, len(columns))
	for _, c := range columns {
		quotedCols = append(quotedCols, database.QuoteIdent(c))
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? AND %s <= ?",
		strings.Join(quotedCols, ", "),
		database.QuoteTable(tableConfig.Source),
		database.QuoteIdent(splitColumn),
		database.QuoteIdent(splitColumn),
	)
	if tableConfig.Where != "" {
		query += " AND " + tableConfig.Where
	}
	query += fmt.Sprintf(" ORDER BY %s", database.QuoteIdent(splitColumn))

	queryCtx, cancel := context.WithTimeout(e.ctx, 15*time.Minute)
	defer cancel()

	rows, err := conn.QueryContext(queryCtx, query, task.Start, task.End)
	if err != nil {
		return fmt.Errorf("query failed (range %d-%d): %w", task.Start, task.End, err)
	}
	defer rows.Close()

	batch := make([][]interface{}, 0, e.config.Sync.ReadBuffer)

	for rows.Next() {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-queryCtx.Done():
			return fmt.Errorf("query timeout (range %d-%d)", task.Start, task.End)
		default:
		}

		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("scan row failed: %w", err)
		}
		batch = append(batch, values)

		if len(batch) >= e.config.Sync.ReadBuffer {
			mem := estimateBatchMemory(batch)
			e.stats.UpdateTableBuffer(tableConfig.Source, len(batch), mem)
			select {
			case dataChan <- batch:
			case <-e.ctx.Done():
				return e.ctx.Err()
			case <-queryCtx.Done():
				return fmt.Errorf("send timeout (range %d-%d)", task.Start, task.End)
			}
			batch = nil
			batch = make([][]interface{}, 0, e.config.Sync.ReadBuffer)
		}
	}

	if len(batch) > 0 {
		mem := estimateBatchMemory(batch)
		e.stats.UpdateTableBuffer(tableConfig.Source, len(batch), mem)
		select {
		case dataChan <- batch:
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-queryCtx.Done():
			return fmt.Errorf("send timeout (range %d-%d)", task.Start, task.End)
		}
	}

	return rows.Err()
}

func (e *Engine) tableReaderSequential(tableConfig config.TableConfig, columns []string, splitColumn string, task SplitTask, tableStats *monitor.TableStats) error {
	quotedCols := make([]string, 0, len(columns))
	for _, c := range columns {
		quotedCols = append(quotedCols, database.QuoteIdent(c))
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? AND %s <= ?",
		strings.Join(quotedCols, ", "),
		database.QuoteTable(tableConfig.Source),
		database.QuoteIdent(splitColumn),
		database.QuoteIdent(splitColumn),
	)
	if tableConfig.Where != "" {
		query += " AND " + tableConfig.Where
	}
	query += fmt.Sprintf(" ORDER BY %s", database.QuoteIdent(splitColumn))

	queryCtx, cancel := context.WithTimeout(e.ctx, 30*time.Minute)
	defer cancel()

	rows, err := e.sourceDB.DB.QueryContext(queryCtx, query, task.Start, task.End)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([][]interface{}, 0, e.config.Sync.BatchSize)
	for rows.Next() {
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-queryCtx.Done():
			return fmt.Errorf("query timeout (range %d-%d)", task.Start, task.End)
		default:
		}
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}
		batch = append(batch, values)
		if len(batch) >= e.config.Sync.BatchSize {
			if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
				return err
			}
			e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
			batch = nil
			batch = make([][]interface{}, 0, e.config.Sync.BatchSize)
		}
	}
	if len(batch) > 0 {
		if err := e.insertBatch(tableConfig.Target, columns, batch); err != nil {
			return err
		}
		e.stats.UpdateTableProgress(tableConfig.Source, int64(len(batch)))
	}
	_ = tableStats
	return rows.Err()
}

func (e *Engine) tableWriter(tableConfig config.TableConfig, columns []string, dataChan <-chan [][]interface{}, wg *sync.WaitGroup, writerErrors chan<- error, tableStats *monitor.TableStats) {
	defer wg.Done()

	for batch := range dataChan {
		select {
		case <-e.ctx.Done():
			return
		default:
		}

		batchRowCount := int64(len(batch))
		batchMemory := estimateBatchMemory(batch)
		e.stats.ResetTableBuffer(tableConfig.Source, batchRowCount, batchMemory)

		err := database.RetryOperation(e.config.Sync.MaxRetries, e.config.GetRetryInterval(), func() error {
			return e.insertBatch(tableConfig.Target, columns, batch)
		})
		if err != nil {
			writerErrors <- err
			logger.Error("  [%s] Writer stopped due to error: %v", tableConfig.Source, err)
			for remaining := range dataChan {
				_ = remaining
			}
			return
		}

		e.stats.UpdateTableProgress(tableConfig.Source, batchRowCount)
	}
	_ = tableStats
}

func (e *Engine) insertBatch(table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	maxParams := 60000
	colCount := len(columns)
	maxRowsPerBatch := maxParams / colCount
	if maxRowsPerBatch < 1 {
		maxRowsPerBatch = 1
	}

	for start := 0; start < len(rows); start += maxRowsPerBatch {
		end := start + maxRowsPerBatch
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]

		query, args, err := e.buildBatchInsert(table, columns, batch)
		if err != nil {
			return err
		}

		_, err = e.targetDB.DB.ExecContext(e.ctx, query, args...)
		if err != nil {
			fullSQL := buildFullSQL(query, args)
			logSQL := fullSQL
			if len(logSQL) > 1000 {
				logSQL = logSQL[:1000] + "..."
			}
			logger.Error("  [INSERT FAILED] Table: %s, Rows: %d, Error: %v", table, len(batch), err)
			logger.Error("  [SQL] %s", logSQL)
			saveFailedSQL(table, fullSQL)
			return err
		}
	}
	return nil
}

func (e *Engine) buildBatchInsert(table string, columns []string, rows [][]interface{}) (string, []interface{}, error) {
	var sb strings.Builder

	estimatedSize := len(rows) * len(columns) * 20
	if estimatedSize > 256*1024 {
		estimatedSize = 256 * 1024
	}
	if estimatedSize < 1024 {
		estimatedSize = 1024
	}
	sb.Grow(estimatedSize)

	quotedCols := make([]string, 0, len(columns))
	for _, c := range columns {
		quotedCols = append(quotedCols, database.QuoteIdent(c))
	}

	sb.WriteString("INSERT INTO ")
	sb.WriteString(database.QuoteTable(table))
	sb.WriteString(" (")
	sb.WriteString(strings.Join(quotedCols, ", "))
	sb.WriteString(") VALUES ")

	args := make([]interface{}, 0, len(rows)*len(columns))
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	placeholderStr := "(" + strings.Join(placeholders, ", ") + ")"

	valueStrs := make([]string, 0, len(rows))
	for _, row := range rows {
		valueStrs = append(valueStrs, placeholderStr)
		args = append(args, row...)
	}
	sb.WriteString(strings.Join(valueStrs, ", "))

	return sb.String(), args, nil
}

func buildFullSQL(query string, args []interface{}) string {
	fullSQL := query
	for _, arg := range args {
		var valStr string
		switch v := arg.(type) {
		case string:
			valStr = "'" + escapeSQLString(v) + "'"
		case []byte:
			valStr = "'" + escapeSQLString(string(v)) + "'"
		case nil:
			valStr = "NULL"
		case time.Time:
			valStr = "'" + v.Format("2006-01-02 15:04:05.000") + "'"
		default:
			valStr = fmt.Sprintf("%v", v)
		}
		fullSQL = strings.Replace(fullSQL, "?", valStr, 1)
	}
	return fullSQL
}

func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, "'", "''")
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return s
}

func saveFailedSQL(tableName string, sqlText string) {
	cleanTableName := tableName
	if idx := strings.LastIndex(tableName, "."); idx != -1 {
		cleanTableName = tableName[idx+1:]
	}
	timestamp := time.Now().Format("150405")
	filename := fmt.Sprintf("%s_failed_%s.sql", cleanTableName, timestamp)

	content := fmt.Sprintf("-- Failed SQL for table: %s\n", tableName)
	content += fmt.Sprintf("-- Generated at: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	content += fmt.Sprintf("-- SQL Length: %d bytes\n\n", len(sqlText))
	content += sqlText

	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		logger.Error("  [saveFailedSQL] Failed to write file %s: %v", filename, err)
	} else {
		logger.Error("  [saveFailedSQL] SQL saved to: %s", filename)
	}
}

func estimateBatchMemory(batch [][]interface{}) int64 {
	if len(batch) == 0 {
		return 0
	}
	var total int64
	for _, row := range batch {
		total += 64
		for _, v := range row {
			total += estimateValueSize(v)
		}
	}
	return total
}

func estimateValueSize(val interface{}) int64 {
	if val == nil {
		return 8
	}
	switch v := val.(type) {
	case string:
		return int64(24 + len(v))
	case []byte:
		return int64(24 + len(v))
	case int64, int, uint64, uint:
		return 8
	case int32, int16, int8, uint32, uint16, uint8:
		return 4
	case float64:
		return 8
	case float32:
		return 4
	case bool:
		return 1
	case time.Time:
		return 24
	default:
		_ = v
		return 32
	}
}

func (e *Engine) checkLargeColumns(tableName string, ts *schema.TableSchema) {
	var largeCols []string
	for _, col := range ts.Columns {
		t := strings.ToLower(col.DataType)
		if strings.Contains(t, "text") || strings.Contains(t, "blob") || t == "json" {
			largeCols = append(largeCols, fmt.Sprintf("%s(%s)", col.Name, col.ColumnType))
		}
	}
	if len(largeCols) > 0 {
		logger.Warn("  [%s] WARNING: Table has %d large column(s) that may cause high memory usage:", tableName, len(largeCols))
		for _, c := range largeCols {
			logger.Warn("    - %s", c)
		}
		logger.Warn("  [%s] Consider reducing 'read_buffer' and 'batch_size' in config", tableName)
	}
}

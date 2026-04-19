package monitor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huayumicheng/MySQL2OBSync/internal/logger"
)

type SyncStats struct {
	StartTime       time.Time
	EndTime         time.Time
	TotalTables     int
	CompletedTables int64
	FailedTables    int64
	TotalRows       int64
	SyncedRows      int64
	FailedRows      int64

	activeTables       sync.Map
	failedTableDetails sync.Map

	mu             sync.RWMutex
	lastSyncedRows int64
	lastUpdateTime time.Time
	stopOnce       sync.Once
	stopCh         chan struct{}
}

type TableStats struct {
	TableName      string
	TotalRows      int64
	SyncedRows     int64
	StartTime      time.Time
	IsCompleted    bool
	BufferedRows   int64
	BufferedMemory int64
}

func NewSyncStats() *SyncStats {
	return &SyncStats{lastUpdateTime: time.Now(), stopCh: make(chan struct{})}
}

func (s *SyncStats) Start() {
	s.StartTime = time.Now()
	s.lastUpdateTime = s.StartTime
}

func (s *SyncStats) Stop() {
	s.EndTime = time.Now()
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *SyncStats) StartTable(tableName string, totalRows int64) *TableStats {
	ts := &TableStats{
		TableName: tableName,
		TotalRows: totalRows,
		StartTime: time.Now(),
	}
	s.activeTables.Store(tableName, ts)
	return ts
}

func (s *SyncStats) CompleteTable(tableName string) {
	if ts, ok := s.activeTables.Load(tableName); ok {
		stats := ts.(*TableStats)
		stats.IsCompleted = true
		s.activeTables.Delete(tableName)
		atomic.AddInt64(&s.CompletedTables, 1)
	}
}

func (s *SyncStats) RecordFailedTable(tableName string, errMsg string) {
	s.failedTableDetails.Store(tableName, errMsg)
	atomic.AddInt64(&s.FailedTables, 1)
}

func (s *SyncStats) GetFailedTables() map[string]string {
	failed := make(map[string]string)
	s.failedTableDetails.Range(func(key, value interface{}) bool {
		failed[key.(string)] = value.(string)
		return true
	})
	return failed
}

func (s *SyncStats) UpdateTableProgress(tableName string, synced int64) {
	if ts, ok := s.activeTables.Load(tableName); ok {
		stats := ts.(*TableStats)
		atomic.AddInt64(&stats.SyncedRows, synced)
		atomic.AddInt64(&s.SyncedRows, synced)
	}
}

func (s *SyncStats) UpdateTableBuffer(tableName string, rowsDelta int, memoryDelta int64) {
	if ts, ok := s.activeTables.Load(tableName); ok {
		stats := ts.(*TableStats)
		atomic.AddInt64(&stats.BufferedRows, int64(rowsDelta))
		atomic.AddInt64(&stats.BufferedMemory, memoryDelta)
	}
}

func (s *SyncStats) ResetTableBuffer(tableName string, rows int64, memory int64) {
	if ts, ok := s.activeTables.Load(tableName); ok {
		stats := ts.(*TableStats)
		atomic.AddInt64(&stats.BufferedRows, -rows)
		atomic.AddInt64(&stats.BufferedMemory, -memory)
	}
}

func (s *SyncStats) GetActiveTables() []*TableStats {
	var tables []*TableStats
	s.activeTables.Range(func(key, value interface{}) bool {
		tables = append(tables, value.(*TableStats))
		return true
	})
	return tables
}

func (s *SyncStats) GetProgressPercent() float64 {
	total := atomic.LoadInt64(&s.TotalRows)
	synced := atomic.LoadInt64(&s.SyncedRows)
	if total == 0 {
		return 0
	}
	return float64(synced) / float64(total) * 100
}

func (s *SyncStats) GetSpeed() float64 {
	s.mu.RLock()
	lastTime := s.lastUpdateTime
	lastRows := s.lastSyncedRows
	s.mu.RUnlock()

	now := time.Now()
	duration := now.Sub(lastTime).Seconds()
	if duration < 0.1 {
		return 0
	}

	currentRows := atomic.LoadInt64(&s.SyncedRows)
	if currentRows < lastRows {
		return 0
	}
	return float64(currentRows-lastRows) / duration
}

func (s *SyncStats) GetAverageSpeed() float64 {
	elapsed := s.GetElapsed().Seconds()
	if elapsed < 0.1 {
		return 0
	}
	synced := atomic.LoadInt64(&s.SyncedRows)
	return float64(synced) / elapsed
}

func (s *SyncStats) GetElapsed() time.Duration {
	if s.EndTime.IsZero() {
		return time.Since(s.StartTime)
	}
	return s.EndTime.Sub(s.StartTime)
}

func (s *SyncStats) GetETA() time.Duration {
	return s.GetETAWithSpeed(s.GetAverageSpeed())
}

func (s *SyncStats) GetETAWithSpeed(speed float64) time.Duration {
	if speed <= 0 {
		return 0
	}
	total := atomic.LoadInt64(&s.TotalRows)
	synced := atomic.LoadInt64(&s.SyncedRows)
	remaining := total - synced
	if remaining <= 0 {
		return 0
	}
	return time.Duration(float64(remaining) / speed * float64(time.Second))
}

func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}

func FormatNumber(n int64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.2fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.2fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}

func (s *SyncStats) StartReporting(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.PrintProgress()
		case <-s.stopCh:
			return
		}
	}
}

func (s *SyncStats) PrintProgress() {
	total := atomic.LoadInt64(&s.TotalRows)
	synced := atomic.LoadInt64(&s.SyncedRows)
	progress := s.GetProgressPercent()
	speed := s.GetSpeed()
	avg := s.GetAverageSpeed()
	eta := s.GetETAWithSpeed(avg)

	logger.Info("Overall: %.2f%% | %s/%s rows | Speed: %.0f rows/s | Avg: %.0f rows/s | Elapsed: %s | ETA: %s",
		progress,
		FormatNumber(synced),
		FormatNumber(total),
		speed,
		avg,
		FormatDuration(s.GetElapsed()),
		FormatDuration(eta),
	)

	for _, t := range s.GetActiveTables() {
		tableSynced := atomic.LoadInt64(&t.SyncedRows)
		bufRows := atomic.LoadInt64(&t.BufferedRows)
		bufMem := atomic.LoadInt64(&t.BufferedMemory)
		p := float64(0)
		if t.TotalRows > 0 {
			p = float64(tableSynced) / float64(t.TotalRows) * 100
		}
		logger.Info("  Table: %s | %.2f%% | %s/%s | Buffered: %s rows | BufferMem: %s",
			t.TableName,
			p,
			FormatNumber(tableSynced),
			FormatNumber(t.TotalRows),
			FormatNumber(bufRows),
			formatBytes(bufMem),
		)
	}

	s.mu.Lock()
	s.lastSyncedRows = synced
	s.lastUpdateTime = time.Now()
	s.mu.Unlock()
}

func (s *SyncStats) PrintSummary() {
	total := atomic.LoadInt64(&s.TotalRows)
	synced := atomic.LoadInt64(&s.SyncedRows)
	failedTables := atomic.LoadInt64(&s.FailedTables)
	completedTables := atomic.LoadInt64(&s.CompletedTables)

	logger.Info("\nSummary:")
	logger.Info("  Tables: total=%d completed=%d failed=%d", s.TotalTables, completedTables, failedTables)
	logger.Info("  Rows: total=%s synced=%s", FormatNumber(total), FormatNumber(synced))
	logger.Info("  Time: elapsed=%s avg_speed=%.0f rows/s", FormatDuration(s.GetElapsed()), s.GetAverageSpeed())

	ft := s.GetFailedTables()
	if len(ft) > 0 {
		logger.Info("  Failed tables:")
		for table, msg := range ft {
			logger.Info("    - %s: %s", table, msg)
		}
	}
}

func formatBytes(b int64) string {
	if b < 0 {
		b = 0
	}
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.2fGB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.2fMB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.2fKB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

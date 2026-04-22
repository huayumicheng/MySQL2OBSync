package compare

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/huayumicheng/MySQL2OBSync/internal/database"
)

type fixSQLWriter struct {
	dir string
	mu  sync.Mutex
	fs  map[string]*os.File
	ws  map[string]*bufio.Writer
}

func newFixSQLWriter(dir string) (*fixSQLWriter, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &fixSQLWriter{dir: dir, fs: map[string]*os.File{}, ws: map[string]*bufio.Writer{}}, nil
}

func (w *fixSQLWriter) Close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, bw := range w.ws {
		_ = bw.Flush()
	}
	for _, f := range w.fs {
		_ = f.Close()
	}
}

func (w *fixSQLWriter) fileFor(table string) (*bufio.Writer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if bw, ok := w.ws[table]; ok {
		return bw, nil
	}
	name := sanitizeFileName(table) + ".sql"
	p := filepath.Join(w.dir, name)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	w.fs[table] = f
	w.ws[table] = bw
	return bw, nil
}

func sanitizeFileName(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, ":", "_")
	s = strings.ReplaceAll(s, ".", "_")
	if s == "" {
		return "fix_sql"
	}
	return s
}

func (w *fixSQLWriter) WriteReplaceOrRebuild(targetTable string, keyCol string, keyType string, cols []string, key interface{}, rows [][]interface{}) {
	if w == nil {
		return
	}
	if len(cols) == 0 {
		return
	}
	bw, err := w.fileFor(targetTable)
	if err != nil {
		return
	}

	if keyType == "SPLIT_COLUMN" {
		del := fmt.Sprintf("DELETE FROM %s WHERE %s = %s;\n", database.QuoteTable(targetTable), database.QuoteIdent(keyCol), sqlLiteral(key))
		_, _ = bw.WriteString(del)
		if len(rows) == 0 {
			_ = bw.Flush()
			return
		}
		w.writeInsert(bw, "INSERT", targetTable, cols, rows)
		_ = bw.Flush()
		return
	}

	if len(rows) == 0 {
		_ = bw.Flush()
		return
	}
	w.writeInsert(bw, "REPLACE", targetTable, cols, rows)
	_ = bw.Flush()
}

func (w *fixSQLWriter) writeInsert(bw *bufio.Writer, verb string, table string, cols []string, rows [][]interface{}) {
	quotedCols := make([]string, 0, len(cols))
	for _, c := range cols {
		quotedCols = append(quotedCols, database.QuoteIdent(c))
	}

	const maxRowsPerStmt = 500
	for start := 0; start < len(rows); start += maxRowsPerStmt {
		end := start + maxRowsPerStmt
		if end > len(rows) {
			end = len(rows)
		}
		part := rows[start:end]

		var sb strings.Builder
		sb.Grow(1024)
		sb.WriteString(verb)
		sb.WriteString(" INTO ")
		sb.WriteString(database.QuoteTable(table))
		sb.WriteString(" (")
		sb.WriteString(strings.Join(quotedCols, ", "))
		sb.WriteString(") VALUES ")

		valParts := make([]string, 0, len(part))
		for _, r := range part {
			valParts = append(valParts, "("+rowLiterals(r)+")")
		}
		sb.WriteString(strings.Join(valParts, ", "))
		sb.WriteString(";\n")
		_, _ = bw.WriteString(sb.String())
	}
}

func rowLiterals(row []interface{}) string {
	out := make([]string, 0, len(row))
	for _, v := range row {
		out = append(out, sqlLiteral(v))
	}
	return strings.Join(out, ", ")
}

func sqlLiteral(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case []byte:
		return quoteString(string(x))
	case string:
		return quoteString(x)
	case time.Time:
		return quoteString(x.UTC().Format("2006-01-02 15:04:05.000000"))
	case bool:
		if x {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(x)
	}
}

func quoteString(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	s = strings.ReplaceAll(s, "\x00", "")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	return "'" + s + "'"
}

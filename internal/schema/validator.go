package schema

import (
	"database/sql"
	"sort"
	"strings"

	"github.com/huayumicheng/MySQL2OBSync/internal/config"
	"github.com/huayumicheng/MySQL2OBSync/internal/logger"
)

type Validator struct {
	sourceDB *sql.DB
	targetDB *sql.DB
}

func NewValidator(sourceDB, targetDB *sql.DB) *Validator {
	return &Validator{sourceDB: sourceDB, targetDB: targetDB}
}

type ColumnDiff struct {
	ColumnName string
	SourceType string
	TargetType string
	DiffType   string
}

type IndexDiff struct {
	IndexName string
	DiffType  string
}

type ConstraintDiff struct {
	ConstraintName string
	DiffType       string
}

type ValidationResult struct {
	TableName       string
	MissingInTarget bool
	ColumnDiffs     []ColumnDiff
	IndexDiffs      []IndexDiff
	ConstraintDiffs []ConstraintDiff
}

func (vr *ValidationResult) IsValid() bool {
	return !vr.MissingInTarget &&
		len(vr.ColumnDiffs) == 0 &&
		len(vr.IndexDiffs) == 0 &&
		len(vr.ConstraintDiffs) == 0
}

func (v *Validator) ValidateAllTables(tables []config.TableConfig) ([]ValidationResult, error) {
	results := make([]ValidationResult, 0, len(tables))
	for _, t := range tables {
		logger.Info("Validating table: %s -> %s", t.Source, t.Target)

		src, err := NewDiscovery(v.sourceDB).DiscoverExtendedTable(t.Source)
		if err != nil {
			logger.Error("  Failed to discover source schema: %v", err)
			continue
		}

		res, err := v.ValidateTable(src, t.Target)
		if err != nil {
			logger.Error("  Validation failed: %v", err)
			continue
		}
		results = append(results, *res)
		v.printValidationResult(res)
	}
	return results, nil
}

func (v *Validator) ValidateTable(sourceSchema *ExtendedTableSchema, targetTable string) (*ValidationResult, error) {
	res := &ValidationResult{
		TableName:       targetTable,
		ColumnDiffs:     make([]ColumnDiff, 0),
		IndexDiffs:      make([]IndexDiff, 0),
		ConstraintDiffs: make([]ConstraintDiff, 0),
	}

	exists, err := v.checkTableExists(v.targetDB, targetTable)
	if err != nil {
		return nil, err
	}
	if !exists {
		res.MissingInTarget = true
		return res, nil
	}

	targetSchema, err := NewDiscovery(v.targetDB).DiscoverExtendedTable(targetTable)
	if err != nil {
		return nil, err
	}

	compareColumns(sourceSchema, targetSchema, res)
	compareIndexes(sourceSchema, targetSchema, res)
	compareConstraints(sourceSchema, targetSchema, res)

	return res, nil
}

func (v *Validator) checkTableExists(db *sql.DB, tableName string) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_name = ?
	`
	var cnt int
	if err := db.QueryRow(query, tableName).Scan(&cnt); err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func compareColumns(sourceSchema, targetSchema *ExtendedTableSchema, res *ValidationResult) {
	targetCols := make(map[string]ColumnSchema, len(targetSchema.Columns))
	for _, c := range targetSchema.Columns {
		targetCols[strings.ToLower(c.Name)] = c
	}

	for _, sc := range sourceSchema.Columns {
		tc, ok := targetCols[strings.ToLower(sc.Name)]
		if !ok {
			res.ColumnDiffs = append(res.ColumnDiffs, ColumnDiff{
				ColumnName: sc.Name,
				SourceType: sc.ColumnType,
				TargetType: "",
				DiffType:   "MISSING_IN_TARGET",
			})
			continue
		}

		if !strings.EqualFold(normalizeColumnType(sc.ColumnType), normalizeColumnType(tc.ColumnType)) {
			res.ColumnDiffs = append(res.ColumnDiffs, ColumnDiff{
				ColumnName: sc.Name,
				SourceType: sc.ColumnType,
				TargetType: tc.ColumnType,
				DiffType:   "TYPE_MISMATCH",
			})
		}

		if sc.IsNullable != tc.IsNullable {
			res.ColumnDiffs = append(res.ColumnDiffs, ColumnDiff{
				ColumnName: sc.Name,
				SourceType: nullableString(sc.IsNullable),
				TargetType: nullableString(tc.IsNullable),
				DiffType:   "NULL_MISMATCH",
			})
		}

		sd := ""
		td := ""
		if sc.DefaultValue != nil {
			sd = *sc.DefaultValue
		}
		if tc.DefaultValue != nil {
			td = *tc.DefaultValue
		}
		if normalizeDefault(sd) != normalizeDefault(td) {
			res.ColumnDiffs = append(res.ColumnDiffs, ColumnDiff{
				ColumnName: sc.Name,
				SourceType: sd,
				TargetType: td,
				DiffType:   "DEFAULT_MISMATCH",
			})
		}
	}
}

func compareIndexes(sourceSchema, targetSchema *ExtendedTableSchema, res *ValidationResult) {
	srcMap := make(map[string]IndexSchema, len(sourceSchema.Indexes))
	for _, idx := range sourceSchema.Indexes {
		srcMap[strings.ToLower(idx.Name)] = idx
	}
	tgtMap := make(map[string]IndexSchema, len(targetSchema.Indexes))
	for _, idx := range targetSchema.Indexes {
		tgtMap[strings.ToLower(idx.Name)] = idx
	}

	for name, sidx := range srcMap {
		tidx, ok := tgtMap[name]
		if !ok {
			res.IndexDiffs = append(res.IndexDiffs, IndexDiff{IndexName: sidx.Name, DiffType: "MISSING_IN_TARGET"})
			continue
		}
		if sidx.IsUnique != tidx.IsUnique || sidx.IsPrimary != tidx.IsPrimary || !sameStringSliceCI(sidx.Columns, tidx.Columns) {
			res.IndexDiffs = append(res.IndexDiffs, IndexDiff{IndexName: sidx.Name, DiffType: "TYPE_MISMATCH"})
		}
	}
}

func compareConstraints(sourceSchema, targetSchema *ExtendedTableSchema, res *ValidationResult) {
	srcKeys := make(map[string]ConstraintSchema, len(sourceSchema.Constraints))
	for _, c := range sourceSchema.Constraints {
		srcKeys[strings.ToLower(c.Name)] = c
	}
	tgtKeys := make(map[string]ConstraintSchema, len(targetSchema.Constraints))
	for _, c := range targetSchema.Constraints {
		tgtKeys[strings.ToLower(c.Name)] = c
	}

	for name, sc := range srcKeys {
		tc, ok := tgtKeys[name]
		if !ok {
			res.ConstraintDiffs = append(res.ConstraintDiffs, ConstraintDiff{ConstraintName: sc.Name, DiffType: "MISSING_IN_TARGET"})
			continue
		}
		if !strings.EqualFold(sc.Type, tc.Type) || !sameStringSliceCI(sc.Columns, tc.Columns) || !strings.EqualFold(sc.RefTable, tc.RefTable) || !sameStringSliceCI(sc.RefColumns, tc.RefColumns) {
			res.ConstraintDiffs = append(res.ConstraintDiffs, ConstraintDiff{ConstraintName: sc.Name, DiffType: "TYPE_MISMATCH"})
		}
	}
}

func (v *Validator) printValidationResult(result *ValidationResult) {
	if result.MissingInTarget {
		logger.Error("  [MISSING] Target table not found: %s", result.TableName)
		return
	}
	if result.IsValid() {
		logger.Info("  [OK] Schema matches")
		return
	}
	if len(result.ColumnDiffs) > 0 {
		logger.Warn("  Column diffs: %d", len(result.ColumnDiffs))
		for _, d := range result.ColumnDiffs {
			logger.Warn("    - %s [%s] source=%s target=%s", d.ColumnName, d.DiffType, d.SourceType, d.TargetType)
		}
	}
	if len(result.IndexDiffs) > 0 {
		logger.Warn("  Index diffs: %d", len(result.IndexDiffs))
		for _, d := range result.IndexDiffs {
			logger.Warn("    - %s [%s]", d.IndexName, d.DiffType)
		}
	}
	if len(result.ConstraintDiffs) > 0 {
		logger.Warn("  Constraint diffs: %d", len(result.ConstraintDiffs))
		for _, d := range result.ConstraintDiffs {
			logger.Warn("    - %s [%s]", d.ConstraintName, d.DiffType)
		}
	}
}

func PrintValidationSummary(results []ValidationResult) {
	var okCount, missingCount, diffCount int
	for _, r := range results {
		if r.MissingInTarget {
			missingCount++
		} else if r.IsValid() {
			okCount++
		} else {
			diffCount++
		}
	}
	logger.Info("\nSchema validation summary: OK=%d, Missing=%d, Diff=%d", okCount, missingCount, diffCount)
}

func normalizeColumnType(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func normalizeDefault(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.ToLower(s), "current_timestamp()")
	if strings.EqualFold(s, "null") {
		return ""
	}
	return s
}

func nullableString(b bool) string {
	if b {
		return "NULL"
	}
	return "NOT NULL"
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

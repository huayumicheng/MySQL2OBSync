package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/huayumicheng/MySQL2OBSync/internal/compare"
	"github.com/huayumicheng/MySQL2OBSync/internal/config"
	"github.com/huayumicheng/MySQL2OBSync/internal/database"
	"github.com/huayumicheng/MySQL2OBSync/internal/logger"
	"github.com/huayumicheng/MySQL2OBSync/internal/schema"
	"github.com/huayumicheng/MySQL2OBSync/internal/sync"
)

var (
	version   = "0.1.0"
	buildTime = "unknown"
)

func main() {
	var (
		configFile     = flag.String("c", "config.yaml", "配置文件路径")
		compareOnly    = flag.Bool("compare-only", false, "仅执行数据对比，跳过同步")
		countOnly      = flag.Bool("count-only", false, "仅对比数据量（行数），跳过同步")
		exportFixSQL   = flag.String("export-fix-sql", "", "导出订正SQL到目录（默认不启用；仅生成不执行）")
		tables         = flag.String("tables", "", "指定要同步/对比的表，逗号分隔")
		showVersion    = flag.Bool("v", false, "显示版本信息")
		createSchema   = flag.Bool("create-schema", false, "仅同步表结构（从源端拉取DDL并在目标端创建），不同步数据")
		validateSchema = flag.Bool("validate-schema", false, "校验源端和目标端表结构")
		dryRun         = flag.Bool("dry-run", false, "仅生成建表脚本，不执行")
		genScript      = flag.String("gen-script", "", "生成建表脚本到指定文件")
		logDir         = flag.String("log-dir", "", "日志目录（默认为可执行文件目录）")
		tableWorkers   = flag.Int("table-workers", 0, "并发同步表数量（配置文件中为sync.table_workers）")
	)

	flag.Parse()

	if *showVersion {
		fmt.Printf("MySQL to OceanBase(MySQL) Sync Tool v%s (built %s)\n", version, buildTime)
		os.Exit(0)
	}

	if *countOnly {
		*compareOnly = true
	}
	if strings.TrimSpace(*exportFixSQL) != "" {
		if *countOnly {
			fmt.Fprintf(os.Stderr, "-export-fix-sql 不能与 -count-only 一起使用（count-only 不做内容对比，无法生成订正SQL）\n")
			os.Exit(2)
		}
		if !*compareOnly {
			fmt.Fprintf(os.Stderr, "-export-fix-sql 必须配合 -compare-only 使用（避免进入数据同步流程）\n")
			os.Exit(2)
		}
	}

	exePath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get executable path: %v\n", err)
		os.Exit(1)
	}
	logPath := filepath.Dir(exePath)
	if *logDir != "" {
		logPath = *logDir
	}
	if err := logger.InitWithDir(logPath); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		logger.Fatal("Failed to load config: %v", err)
	}

	if *tableWorkers > 0 {
		cfg.Sync.TableWorkers = *tableWorkers
	}
	if *countOnly {
		cfg.Compare.CountOnly = true
	}
	if *compareOnly && !*countOnly {
		cfg.Compare.CountOnly = false
	}

	logger.Info("MySQL to OceanBase(MySQL) Sync Tool v%s", version)
	logger.Info("Job: %s", cfg.JobName)
	logger.Info("Source: %s@%s:%d/%s", cfg.Source.Username, cfg.Source.Host, cfg.Source.Port, cfg.Source.Database)
	logger.Info("Target: %s@%s:%d/%s", cfg.Target.Username, cfg.Target.Host, cfg.Target.Port, cfg.Target.Database)

	if *tables != "" {
		tableList := strings.Split(*tables, ",")
		cfg.Tables = make([]config.TableConfig, 0, len(tableList))
		for _, t := range tableList {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			cfg.Tables = append(cfg.Tables, config.TableConfig{
				Source:             t,
				Target:             t,
				TruncateBeforeSync: true,
			})
		}
	}

	sourceDB, err := database.NewMySQLConnection(cfg.Source)
	if err != nil {
		logger.Fatal("Failed to connect to source: %v", err)
	}
	defer sourceDB.Close()
	logger.Info("✓ Connected to source database")

	targetDB, err := database.NewMySQLConnection(cfg.Target)
	if err != nil {
		logger.Fatal("Failed to connect to target: %v", err)
	}
	defer targetDB.Close()
	logger.Info("✓ Connected to target database")

	if *compareOnly {
		runCompare(cfg, sourceDB, targetDB, *exportFixSQL)
		return
	}
	if *createSchema {
		runCreateSchema(cfg, sourceDB, targetDB, *dryRun, *genScript)
		return
	}
	if *validateSchema {
		runValidateSchema(cfg, sourceDB, targetDB)
		return
	}

	runSync(cfg, sourceDB, targetDB, *exportFixSQL)
}

func runSync(cfg *config.Config, sourceDB, targetDB *database.Connection, exportFixSQLDir string) {
	logger.Info("\nStarting sync with %d workers, batch size: %d, table workers: %d",
		cfg.Sync.Workers, cfg.Sync.BatchSize, cfg.Sync.TableWorkers)

	engine := sync.NewEngine(cfg, sourceDB, targetDB)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Fatal("Sync panic: %v", r)
			}
		}()
		done <- engine.Run()
	}()

	select {
	case err := <-done:
		if err != nil {
			logger.Fatal("\nSync failed: %v", err)
		}
	case sig := <-sigChan:
		logger.Info("\nReceived signal: %v, stopping...", sig)
		engine.Stop()
		<-done
	}

	engine.GetStats().PrintSummary()

	if cfg.Compare.AutoCompare {
		logger.Info("\nRunning automatic data comparison...")
		runCompare(cfg, sourceDB, targetDB, exportFixSQLDir)
	}

	logger.Info("\n✓ Sync completed successfully!")
}

func runCompare(cfg *config.Config, sourceDB, targetDB *database.Connection, exportFixSQLDir string) {
	logger.Info("Starting data comparison...")

	tablePairs := make([]compare.TablePair, 0, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tablePairs = append(tablePairs, compare.TablePair{
			Source:      t.Source,
			Target:      t.Target,
			SplitColumn: t.SplitColumn,
			CountOnly:   cfg.Compare.CountOnly,
			BatchKeys:   cfg.Compare.BatchKeys,
		})
	}

	if len(tablePairs) == 0 {
		logger.Info("Discovering tables...")
		tables, err := schema.NewDiscovery(sourceDB.DB).DiscoverAllTables()
		if err != nil {
			logger.Error("Failed to discover tables: %v", err)
			return
		}
		for _, t := range tables {
			tablePairs = append(tablePairs, compare.TablePair{
				Source:    t,
				Target:    t,
				CountOnly: cfg.Compare.CountOnly,
				BatchKeys: cfg.Compare.BatchKeys,
			})
		}
	}

	comparator := compare.NewComparator(
		sourceDB,
		targetDB,
		cfg.Compare.CompareWorkers,
		cfg.Compare.CheckpointFile,
		cfg.Compare.DrillThreshold,
		exportFixSQLDir,
	)
	results, err := comparator.CompareTables(tablePairs)
	if err != nil {
		logger.Fatal("Compare failed: %v", err)
	}
	compare.PrintCompareSummary(results)
}

func runCreateSchema(cfg *config.Config, sourceDB, targetDB *database.Connection, dryRun bool, scriptFile string) {
	logger.Info("\nCreating schema for tables...")
	if dryRun {
		logger.Info("(Dry run mode - no changes will be made)")
	}

	var tables []string
	if len(cfg.Tables) == 0 {
		all, err := schema.NewDiscovery(sourceDB.DB).DiscoverAllTables()
		if err != nil {
			logger.Fatal("Failed to discover tables: %v", err)
		}
		tables = all
	} else {
		for _, t := range cfg.Tables {
			tables = append(tables, t.Source)
		}
	}

	creator := schema.NewCreator(sourceDB.DB, targetDB.DB, cfg.Schema.StripAutoIncrement)
	if scriptFile != "" {
		if err := creator.SaveCreateTableScript(tables, scriptFile); err != nil {
			logger.Fatal("Failed to write script file: %v", err)
		}
		logger.Info("\n✓ Script saved to: %s", scriptFile)
		return
	}

	if err := creator.CreateSchemaForTables(tables, dryRun); err != nil {
		logger.Fatal("\nSchema creation failed: %v", err)
	}

	if dryRun {
		logger.Info("\n✓ Dry run completed")
	} else {
		logger.Info("\n✓ Schema created successfully!")
	}
}

func runValidateSchema(cfg *config.Config, sourceDB, targetDB *database.Connection) {
	logger.Info("\nValidating schema...")

	tables := cfg.Tables
	if len(tables) == 0 {
		all, err := schema.NewDiscovery(sourceDB.DB).DiscoverAllTables()
		if err != nil {
			logger.Fatal("Failed to discover tables: %v", err)
		}
		for _, t := range all {
			tables = append(tables, config.TableConfig{Source: t, Target: t})
		}
	}

	validator := schema.NewValidator(sourceDB.DB, targetDB.DB)
	results, err := validator.ValidateAllTables(tables)
	if err != nil {
		logger.Fatal("Validation failed: %v", err)
	}
	schema.PrintValidationSummary(results)
}

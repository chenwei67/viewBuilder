package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type TimestampColumnConfig struct {
	Column string `json:"column"`
	Type   string `json:"type"`
}

type Config struct {
	Host                   string                           `json:"host"`
	Port                   int                              `json:"port"`
	Username               string                           `json:"username"`
	Password               string                           `json:"password"`
	Database               string                           `json:"database"`
	DefaultTimestampColumn TimestampColumnConfig            `json:"default_timestamp_column"`
	TimestampColumns       map[string]TimestampColumnConfig `json:"timestamp_columns"`
	T0                     string                           `json:"t0"`
	OldTableSuffix         string                           `json:"old_table_suffix"`
	NewTableSuffix         string                           `json:"new_table_suffix"`
}

type t0Value struct {
	Time       time.Time
	UnixSecond int64
	HasTime    bool
	HasUnix    bool
}

func parseConfigFile(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	if cfg.Host == "" || cfg.Port == 0 || cfg.Username == "" || cfg.Database == "" {
		return nil, fmt.Errorf("配置缺少必要字段: host/port/username/database")
	}
	if strings.TrimSpace(cfg.DefaultTimestampColumn.Column) == "" {
		cfg.DefaultTimestampColumn.Column = "recordTimestamp"
	}
	if strings.TrimSpace(cfg.DefaultTimestampColumn.Type) == "" {
		cfg.DefaultTimestampColumn.Type = "bigint"
	}
	oldSuffix := strings.TrimSpace(cfg.OldTableSuffix)
	newSuffix := strings.TrimSpace(cfg.NewTableSuffix)
	if oldSuffix == "" && newSuffix == "" {
		cfg.OldTableSuffix = "_1"
		cfg.NewTableSuffix = "_2"
	} else if oldSuffix == "" || newSuffix == "" {
		return nil, fmt.Errorf("old_table_suffix/new_table_suffix 必须同时配置或同时为空")
	} else if oldSuffix == newSuffix {
		return nil, fmt.Errorf("old_table_suffix/new_table_suffix 不能相同")
	}
	if strings.TrimSpace(cfg.T0) == "" {
		return nil, fmt.Errorf("配置缺少 t0")
	}
	return &cfg, nil
}

func openStarRocks(cfg *Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=10s&readTimeout=30s&writeTimeout=30s&parseTime=true&loc=Local",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(2 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func parseT0(s string) (t0Value, error) {
	trimmed := strings.TrimSpace(strings.Trim(s, "'"))
	if trimmed == "" {
		return t0Value{}, fmt.Errorf("t0 为空")
	}
	if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return t0Value{UnixSecond: n, HasUnix: true, Time: time.Unix(n, 0), HasTime: true}, nil
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339,
	}
	var lastErr error
	for _, layout := range layouts {
		tm, err := time.ParseInLocation(layout, trimmed, time.Local)
		if err == nil {
			return t0Value{Time: tm, HasTime: true, UnixSecond: tm.Unix(), HasUnix: true}, nil
		}
		lastErr = err
	}
	return t0Value{}, fmt.Errorf("t0 解析失败(%s): %w", trimmed, lastErr)
}

func t0LiteralForType(t0 t0Value, tsType string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(tsType)) {
	case "bigint":
		if !t0.HasUnix {
			return "", fmt.Errorf("t0 无法转为 unix seconds")
		}
		return strconv.FormatInt(t0.UnixSecond, 10), nil
	case "datetime":
		if !t0.HasTime {
			return "", fmt.Errorf("t0 无法转为 datetime")
		}
		return "'" + t0.Time.Format("2006-01-02 15:04:05") + "'", nil
	case "date":
		if !t0.HasTime {
			return "", fmt.Errorf("t0 无法转为 date")
		}
		return "'" + t0.Time.Format("2006-01-02") + "'", nil
	default:
		return "", fmt.Errorf("不支持的时间列类型: %s (仅支持 bigint/date/datetime)", tsType)
	}
}

func listTables(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	const q = "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"
	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func getTableType(ctx context.Context, db *sql.DB, schema, name string) (string, error) {
	const q = "SELECT table_type FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
	var t string
	if err := db.QueryRowContext(ctx, q, schema, name).Scan(&t); err != nil {
		return "", err
	}
	return strings.ToUpper(strings.TrimSpace(t)), nil
}

func getColumns(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	const q = "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position"
	rows, err := db.QueryContext(ctx, q, schema, table)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("表 %s.%s 没有列或不存在", schema, table)
	}
	return cols, nil
}

func validateSameColumns(oldCols, newCols []string) error {
	if len(oldCols) != len(newCols) {
		return fmt.Errorf("列数量不一致: old=%d new=%d", len(oldCols), len(newCols))
	}
	for i := range oldCols {
		if oldCols[i] != newCols[i] {
			return fmt.Errorf("列不一致: idx=%d old=%s new=%s", i, oldCols[i], newCols[i])
		}
	}
	return nil
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func buildUnionViewSQL(schema, viewName, oldTable, newTable string, columns []string, tsColumn, t0Literal string) string {
	var b strings.Builder
	b.WriteString("select\n")
	for i, c := range columns {
		b.WriteString("\t")
		b.WriteString(quoteIdent(c))
		if i == len(columns)-1 {
			b.WriteString("\n")
		} else {
			b.WriteString(",\n")
		}
	}
	oldSel := b.String()
	var b2 strings.Builder
	b2.WriteString(oldSel)
	b2.WriteString("from ")
	b2.WriteString(quoteIdent(schema))
	b2.WriteString(".")
	b2.WriteString(quoteIdent(oldTable))
	b2.WriteString("\nwhere ")
	b2.WriteString(quoteIdent(tsColumn))
	b2.WriteString(" <= ")
	b2.WriteString(t0Literal)

	var b3 strings.Builder
	b3.WriteString(oldSel)
	b3.WriteString("from ")
	b3.WriteString(quoteIdent(schema))
	b3.WriteString(".")
	b3.WriteString(quoteIdent(newTable))
	b3.WriteString("\nwhere ")
	b3.WriteString(quoteIdent(tsColumn))
	b3.WriteString(" > ")
	b3.WriteString(t0Literal)

	return fmt.Sprintf("create view %s.%s as\n%s\nunion all\n%s;\n",
		quoteIdent(schema),
		quoteIdent(viewName),
		b2.String(),
		b3.String(),
	)
}

func buildAlterViewSQL(schema, viewName, unionQuery string) (string, error) {
	s := strings.TrimSpace(unionQuery)
	prefix := fmt.Sprintf("create view %s.%s as", quoteIdent(schema), quoteIdent(viewName))
	if !strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix)) {
		return "", fmt.Errorf("无法从 create view SQL 转换为 alter view")
	}
	rest := strings.TrimSpace(s[len(prefix):])
	return fmt.Sprintf("alter view %s.%s as\n%s\n", quoteIdent(schema), quoteIdent(viewName), rest), nil
}

func timestampConfigForTable(cfg *Config, baseTable string) TimestampColumnConfig {
	if cfg.TimestampColumns != nil {
		if c, ok := cfg.TimestampColumns[baseTable]; ok {
			if strings.TrimSpace(c.Column) != "" && strings.TrimSpace(c.Type) != "" {
				return c
			}
			if strings.TrimSpace(c.Column) != "" {
				return TimestampColumnConfig{Column: c.Column, Type: cfg.DefaultTimestampColumn.Type}
			}
			if strings.TrimSpace(c.Type) != "" {
				return TimestampColumnConfig{Column: cfg.DefaultTimestampColumn.Column, Type: c.Type}
			}
		}
	}
	return cfg.DefaultTimestampColumn
}

func run(ctx context.Context, cfg *Config, dryRun bool) error {
	t0, err := parseT0(cfg.T0)
	if err != nil {
		return err
	}
	db, err := openStarRocks(cfg)
	if err != nil {
		return fmt.Errorf("连接 StarRocks 失败: %w", err)
	}
	defer db.Close()

	tables, err := listTables(ctx, db, cfg.Database)
	if err != nil {
		return fmt.Errorf("获取表列表失败: %w", err)
	}
	oldByBase := make(map[string]string)
	newByBase := make(map[string]string)
	for _, t := range tables {
		if strings.HasSuffix(t, cfg.OldTableSuffix) {
			base := strings.TrimSuffix(t, cfg.OldTableSuffix)
			if base != "" {
				oldByBase[base] = t
			}
		}
		if strings.HasSuffix(t, cfg.NewTableSuffix) {
			base := strings.TrimSuffix(t, cfg.NewTableSuffix)
			if base != "" {
				newByBase[base] = t
			}
		}
	}

	var bases []string
	for base := range oldByBase {
		if _, ok := newByBase[base]; ok {
			bases = append(bases, base)
		}
	}
	sort.Strings(bases)
	if len(bases) == 0 {
		return fmt.Errorf("未找到可配对的表 (old_suffix=%s new_suffix=%s)", cfg.OldTableSuffix, cfg.NewTableSuffix)
	}

	for _, base := range bases {
		oldTable := oldByBase[base]
		newTable := newByBase[base]

		oldCols, err := getColumns(ctx, db, cfg.Database, oldTable)
		if err != nil {
			return err
		}
		newCols, err := getColumns(ctx, db, cfg.Database, newTable)
		if err != nil {
			return err
		}
		if err := validateSameColumns(oldCols, newCols); err != nil {
			return fmt.Errorf("表 %s 列对齐校验失败: %w", base, err)
		}

		tsCfg := timestampConfigForTable(cfg, base)
		t0Lit, err := t0LiteralForType(t0, tsCfg.Type)
		if err != nil {
			return fmt.Errorf("表 %s 时间条件生成失败: %w", base, err)
		}

		createSQL := buildUnionViewSQL(cfg.Database, base, oldTable, newTable, oldCols, tsCfg.Column, t0Lit)
		if dryRun {
			fmt.Printf("---- %s ----\n%s\n", base, createSQL)
			continue
		}

		tt, err := getTableType(ctx, db, cfg.Database, base)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, execErr := db.ExecContext(ctx, createSQL); execErr != nil {
					return fmt.Errorf("创建视图失败(%s): %w", base, execErr)
				}
				continue
			}
			return fmt.Errorf("读取对象类型失败(%s): %w", base, err)
		}

		if tt == "VIEW" {
			alterSQL, err := buildAlterViewSQL(cfg.Database, base, createSQL)
			if err != nil {
				return fmt.Errorf("构造 ALTER VIEW 失败(%s): %w", base, err)
			}
			if _, execErr := db.ExecContext(ctx, alterSQL); execErr != nil {
				return fmt.Errorf("更新视图失败(%s): %w", base, execErr)
			}
			continue
		}

		if _, execErr := db.ExecContext(ctx, createSQL); execErr != nil {
			return fmt.Errorf("创建视图失败(%s): %w", base, execErr)
		}
	}

	return nil
}

func main() {
	var configPath string
	var dryRun bool
	flag.StringVar(&configPath, "config", "./config.json", "配置文件路径")
	flag.BoolVar(&dryRun, "dry-run", false, "仅输出SQL，不执行")
	flag.Parse()

	cfg, err := parseConfigFile(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := run(ctx, cfg, dryRun); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

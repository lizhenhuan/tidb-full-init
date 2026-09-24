// tidb-ddlsync TiDB → Oracle DDL 表结构同步 CLI（#t8 P1）。
//
// 两种模式：
//
//	-dry-run：只读预览——探测 Oracle 版本/标识符上限、兼容预检、
//	           生成 DDL 与兼容项报告（不执行任何写语句）；
//	直写模式：逐表执行 DDL（单表失败不中断其余表，退出码判定）。
//
// 输出：DDL 文本（-sql-out，默认 ddl-sync.sql）+ JSON 报告
// （-report-out，默认 ddl-sync-report.json；含 OracleVer/MaxIdent
// 验收首查项与逐表 compat[]）。
//
// 退出码：0 = 无 failed（skipped 允许）；1 = 存在 failed；2 = 配置/连接错误。
// Oracle 口令可用环境变量 ORACLE_FREE_PASSWORD 兜底（leader 口径）。
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	goorav2 "github.com/sijms/go-ora/v2"

	"github.com/lizhenhuan/tidb-full-init/internal/ddlsync"
)

func main() {
	var (
		tidbDSN     = flag.String("tidb-dsn", "root@tcp(127.0.0.1:4000)/", "源 TiDB DSN")
		oraHost     = flag.String("ora-host", "127.0.0.1", "目标 Oracle 主机")
		oraPort     = flag.Int("ora-port", 1521, "目标 Oracle 端口")
		oraService  = flag.String("ora-service", "FREEPDB1", "目标 Oracle service name")
		oraUser     = flag.String("ora-user", "system", "目标 Oracle 用户")
		oraPassword = flag.String("ora-password", "", "目标 Oracle 口令（空则取 ORACLE_FREE_PASSWORD）")

		database     = flag.String("database", "", "源 TiDB 库名（必填）")
		tablesFlag   = flag.String("tables", "", "逗号分隔源表名（必填）")
		targetSchema = flag.String("target-schema", "", "目标 Oracle OWNER（空 = 源库名）")
		nameCase     = flag.String("name-case", "origin", "标识符大小写：origin|lower|upper")
		overwrite    = flag.Bool("overwrite", false, "预置 DROP TABLE（高危：丢目标数据）")
		dryRun       = flag.Bool("dry-run", false, "只读预览：生成 DDL/报告，不执行")
		sqlOut       = flag.String("sql-out", "ddl-sync.sql", "DDL 输出文件")
		reportOut    = flag.String("report-out", "ddl-sync-report.json", "JSON 报告输出文件")
		timeoutSec   = flag.Int("timeout-sec", 600, "整体超时（秒）")
	)
	flag.Parse()

	if *database == "" || *tablesFlag == "" {
		flag.Usage()
		os.Exit(2)
	}
	nc, err := parseNameCase(*nameCase)
	if err != nil {
		log.Fatalf("%v", err)
	}
	var tables []string
	for _, t := range strings.Split(*tablesFlag, ",") {
		if t = strings.TrimSpace(t); t != "" {
			tables = append(tables, t)
		}
	}

	pw := *oraPassword
	if pw == "" {
		pw = os.Getenv("ORACLE_FREE_PASSWORD")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	tidb, err := sql.Open("mysql", *tidbDSN)
	if err != nil {
		log.Fatalf("open tidb: %v", err)
	}
	defer tidb.Close()
	if err := tidb.PingContext(ctx); err != nil {
		log.Fatalf("ping tidb: %v", err)
	}

	oraURL := goorav2.BuildUrl(*oraHost, *oraPort, *oraService, *oraUser, pw, nil)
	ora, err := sql.Open("oracle", oraURL)
	if err != nil {
		log.Fatalf("open oracle: %v", err)
	}
	defer ora.Close()
	if err := ora.PingContext(ctx); err != nil {
		log.Fatalf("ping oracle: %v", err)
	}

	res, err := ddlsync.Run(ctx, tidb, ora, ddlsync.RunConfig{
		Database: *database, Tables: tables, TargetSchema: *targetSchema,
		NameCase: nc, Overwrite: *overwrite, DryRun: *dryRun,
	})
	if err != nil {
		log.Fatalf("run: %v", err)
	}

	if err := writeSQL(*sqlOut, res, *dryRun); err != nil {
		log.Fatalf("write sql: %v", err)
	}
	if err := writeReport(*reportOut, res, *dryRun); err != nil {
		log.Fatalf("write report: %v", err)
	}

	failed := 0
	for _, t := range res.Tables {
		mark := "OK "
		switch t.Status {
		case ddlsync.StatusFailed:
			mark = "FAIL"
			failed++
		case ddlsync.StatusSkipped:
			mark = "SKIP"
		}
		fmt.Printf("%s %-30s %s %s\n", mark, t.Table, t.Status, t.ErrorText)
	}
	fmt.Printf("oracle=%s maxIdent=%d tables=%d failed=%d dryRun=%v\n",
		res.OracleVer, res.MaxIdent, len(res.Tables), failed, *dryRun)
	fmt.Printf("sql=%s report=%s\n", *sqlOut, *reportOut)
	if failed > 0 {
		os.Exit(1)
	}
}

func parseNameCase(s string) (ddlsync.NameCase, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "origin", "":
		return ddlsync.NameCaseOrigin, nil
	case "lower":
		return ddlsync.NameCaseLower, nil
	case "upper":
		return ddlsync.NameCaseUpper, nil
	default:
		return 0, fmt.Errorf("tidb-ddlsync: invalid -name-case %q (origin|lower|upper)", s)
	}
}

// cliReport 报告外壳：dry-run 标注 + 环境首查项置顶。
type cliReport struct {
	DryRun    bool              `json:"dryRun"`
	OracleVer string            `json:"oracleVersion"`
	MaxIdent  int               `json:"maxIdentLen"`
	Tables    []ddlsync.TableResult `json:"tables"`
}

func writeSQL(path string, res *ddlsync.Result, dryRun bool) error {
	var sb strings.Builder
	mode := "EXECUTE"
	if dryRun {
		mode = "DRY-RUN（未执行）"
	}
	fmt.Fprintf(&sb, "-- tidb-ddlsync %s oracle=%s maxIdent=%d\n", mode, res.OracleVer, res.MaxIdent)
	for _, t := range res.Tables {
		fmt.Fprintf(&sb, "\n-- ==== %s [%s] ====\n", t.Table, t.Status)
		if t.DDL != "" {
			sb.WriteString(t.DDL)
			if !strings.HasSuffix(t.DDL, "\n") {
				sb.WriteString("\n")
			}
		} else {
			sb.WriteString("-- （未生成 DDL）\n")
		}
	}
	return os.WriteFile(path, []byte(sb.String()), 0644)
}

func writeReport(path string, res *ddlsync.Result, dryRun bool) error {
	b, err := json.MarshalIndent(cliReport{
		DryRun: dryRun, OracleVer: res.OracleVer, MaxIdent: res.MaxIdent, Tables: res.Tables,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

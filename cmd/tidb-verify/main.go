// tidb-verify：TiDB vs Oracle 一致性比对工具（M2 T2.4，L5 测试基础设施、
// 二期 Verifier 雏形）。全量 count + 分块 checksum + 逐行逐列 diff，
// 大表可 -sample 抽样。旧版「canal JSON 导出」已由 tidb-full-init 主 CLI
// 的快照路径取代，本工具专注比对。
//
// 用法示例：
//
//	tidb-verify \
//	  -tidb-dsn 'root:pwd@tcp(10.168.2.232:4000)/' \
//	  -source lzh_db.orders \
//	  -oracle-host 127.0.0.1 -oracle-port 1521 -oracle-service FREEPDB1 \
//	  -oracle-user system -oracle-password pwd \
//	  -target shop.orders \
//	  [-chunk-size 1000] [-max-diffs 50] [-sample 1000] [-json]
//
// 退出码：0 = 一致；1 = 有差异；2 = 执行错误。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/verify"
)

func main() {
	var (
		tidbDSN     string
		source      string
		oraHost     string
		oraPort     int
		oraService  string
		oraUser     string
		oraPassword string
		target      string
		chunkSize   int
		maxDiffs    int
		sampleN     int
		jsonOut     bool
	)
	flag.StringVar(&tidbDSN, "tidb-dsn", "", "TiDB DSN（如 root:pwd@tcp(host:4000)/，必填）")
	flag.StringVar(&source, "source", "", "源表 db.table（必填）")
	flag.StringVar(&oraHost, "oracle-host", "", "Oracle host（必填）")
	flag.IntVar(&oraPort, "oracle-port", 1521, "Oracle port")
	flag.StringVar(&oraService, "oracle-service", "FREEPDB1", "Oracle service name")
	flag.StringVar(&oraUser, "oracle-user", "system", "Oracle user")
	flag.StringVar(&oraPassword, "oracle-password", "", "Oracle password（建议环境变量 ORACLE_FREE_PASSWORD 传入）")
	flag.StringVar(&target, "target", "", "目标 schema.table（必填）")
	flag.IntVar(&chunkSize, "chunk-size", 1000, "分块 checksum 块大小")
	flag.IntVar(&maxDiffs, "max-diffs", 50, "行级差异最多展开数")
	flag.IntVar(&sampleN, "sample", 0, "抽样比对行数（>0 启用抽样模式，大表用）")
	flag.BoolVar(&jsonOut, "json", false, "JSON 输出报告")
	flag.Parse()

	if oraPassword == "" {
		oraPassword = os.Getenv("ORACLE_FREE_PASSWORD")
	}
	for k, v := range map[string]string{
		"-tidb-dsn": tidbDSN, "-source": source, "-oracle-host": oraHost, "-target": target,
	} {
		if v == "" {
			log.Fatalf("%s is required", k)
		}
	}
	srcDB, srcTbl := splitTable(source)
	dstSchema, dstTbl := splitTable(target)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	src, err := verify.NewTiDBSource(ctx, verify.TiDBConfig{DSN: tidbDSN, Database: srcDB, Table: srcTbl})
	if err != nil {
		log.Fatalf("source: %v", err)
	}
	defer src.Close()
	dst, err := verify.NewOracleSource(ctx, verify.OracleConfig{
		Host: oraHost, Port: oraPort, Service: oraService,
		User: oraUser, Password: oraPassword,
		Schema: dstSchema, Table: dstTbl,
	}, src.PKNames())
	if err != nil {
		log.Fatalf("target: %v", err)
	}
	defer dst.Close()

	var rep *verify.Report
	if sampleN > 0 {
		rep, err = verify.Sample(ctx, src, dst, sampleN, verify.Options{EmptyStringAsNull: true})
	} else {
		rep, err = verify.Compare(ctx, src, dst, verify.Options{ChunkSize: chunkSize, MaxRowDiffs: maxDiffs, EmptyStringAsNull: true})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		os.Exit(2)
	}

	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			log.Fatalf("encode report: %v", err)
		}
	} else {
		printReport(rep)
	}

	if !rep.Equal() {
		os.Exit(1)
	}
}

func splitTable(s string) (string, string) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		log.Fatalf("invalid db.table: %q", s)
	}
	return parts[0], parts[1]
}

func printReport(r *verify.Report) {
	fmt.Printf("source rows : %d\ntarget rows : %d\n", r.SourceRows, r.TargetRows)
	if len(r.Chunks) > 0 {
		fmt.Printf("chunks      : %d checked, %d mismatched\n", len(r.Chunks), r.ChunksMismatched)
		for _, c := range r.Chunks {
			if !c.ChecksumMatch {
				fmt.Printf("  chunk #%d (rows=%d, checksum=%#x): MISMATCH\n", c.Index, c.Rows, c.SourceChecksum)
			}
		}
	}
	fmt.Printf("row diffs   : %d\n", r.RowDiffCount)
	for _, d := range r.RowDiffs {
		for _, c := range d.Columns {
			fmt.Printf("  pk=%s col=%s: source=%q target=%q\n",
				verify.FormatPK(d.PK), c.Column, c.Source, c.Target)
		}
	}
	if r.MissingCount > 0 {
		fmt.Printf("missing in target: %d (e.g. %s)\n", r.MissingCount,
			joinPKs(r.MissingInTarget))
	}
	if r.ExtraCount > 0 {
		fmt.Printf("extra in target  : %d (e.g. %s)\n", r.ExtraCount,
			joinPKs(r.ExtraInTarget))
	}
	fmt.Printf("duration    : %s\n", r.Duration)
	if r.Equal() {
		fmt.Println("result      : CONSISTENT")
	} else {
		fmt.Println("result      : INCONSISTENT")
	}
}

func joinPKs(pks []string) string {
	out := make([]string, 0, len(pks))
	for _, p := range pks {
		out = append(out, verify.FormatPK(p))
	}
	s := strings.Join(out, ", ")
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

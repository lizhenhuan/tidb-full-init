// tidb-console M3 console REST 服务（#t8 P2）：datasources + ddl-sync
// 任务闭环（/jobs /oracle/ddl-preview /jobs/{id}/ddl-rerun）。
//
// 默认监听 :8080（vite dev 代理目标 web/vite.config.ts）；任务库默认
// ./console-data（SQLite，凭据存本地库文件——单机运维工具，文件权限即
// 边界）。Oracle 口令兜底 ORACLE_FREE_PASSWORD 环境变量（datasource
// password 为空时生效）。
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/lizhenhuan/tidb-full-init/internal/api"
	"github.com/lizhenhuan/tidb-full-init/internal/job"
)

func main() {
	addr := flag.String("addr", ":8080", "监听地址")
	dataDir := flag.String("data", "console-data", "任务库目录（SQLite）")
	flag.Parse()

	store, err := job.OpenStore(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer store.Close()

	srv := api.New(store, job.NewManager(store))
	log.Printf("tidb-console listening on %s (data=%s)", *addr, *dataDir)
	if err := http.ListenAndServe(*addr, srv.Mux); err != nil {
		log.Fatal(err)
	}
}

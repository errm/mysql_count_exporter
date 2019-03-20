package main

import (
	"database/sql"
	"flag"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	listTablesQuery = `
          SELECT
	      TABLE_SCHEMA,
	      TABLE_NAME
	    FROM information_schema.tables
	    WHERE TABLE_SCHEMA NOT IN ('sys','mysql','performance_schema','information_schema')
        `
	namespace = "mysql"
	subsystem = "count_exporter"
)

var (
	addr = flag.String("listen-address", ":9557", "The address to listen on for telemetry.")
	path = flag.String("telemetry-path", "/metrics", "Path under which to expose metrics.")
	dsn  = flag.String("dsn", os.Getenv("DATA_SOURCE_NAME"), "A number of seconds to wait before re-counting rows")
)

func NewMysqlCountCollector(dataSourceName string) *MysqlCountCollector {
	return &MysqlCountCollector{
		dsn: dataSourceName,
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "row_count"),
			"Number of rows in the table",
			[]string{"schema", "table"},
			nil,
		),
	}
}

type MysqlCountCollector struct {
	dsn  string
	desc *prometheus.Desc
}

func (c *MysqlCountCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *MysqlCountCollector) Collect(ch chan<- prometheus.Metric) {
	db, err := sql.Open("mysql", c.dsn)
	if err != nil {
		log.Printf("error connecting to database: %v", err)
		return
	}
	defer db.Close()

	tables, err := db.Query(listTablesQuery)
	defer tables.Close()
	if err != nil {
		log.Printf("error listing tables: %v", err)
		return
	}

	var schema string
	var table string
	var count float64

	for tables.Next() {
		if err := tables.Scan(
			&schema,
			&table,
		); err != nil {
			log.Printf("error listing tables: %v", err)
			continue
		}

		row := db.QueryRow("SELECT COUNT(*) FROM " + schema + "." + table)
		if err := row.Scan(&count); err != nil {
			log.Printf("error counting rows: %v", err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, count, schema, table)
	}
}

func main() {
	prometheus.MustRegister(NewMysqlCountCollector(*dsn))
	http.Handle(*path, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, please go to %q for metrics", html.EscapeString(*path))
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}

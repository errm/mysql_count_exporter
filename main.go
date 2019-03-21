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

type MysqlTable struct {
	schema string
	table  string
}

func (c *MysqlCountCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *MysqlCountCollector) Collect(ch chan<- prometheus.Metric) {
	db, err := sql.Open("mysql", c.dsn)
	defer db.Close()

	if err != nil {
		log.Printf("error connecting to database: %v", err)
		return
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	var count float64
	for _, t := range ListTables(db) {
		row := db.QueryRow("SELECT COUNT(*) FROM " + t.schema + "." + t.table)
		if err := row.Scan(&count); err != nil {
			log.Printf("error counting rows: %v", err)
			continue
		}

		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, count, t.schema, t.table)
	}

}

func ListTables(db *sql.DB) []MysqlTable {
	var schema string
	var table string
	var tables []MysqlTable

	list, err := db.Query(listTablesQuery)
	defer list.Close()

	if err != nil {
		log.Printf("error listing tables: %v", err)
		return tables
	}

	for list.Next() {
		if err := list.Scan(
			&schema,
			&table,
		); err != nil {
			log.Printf("error listing tables: %v", err)
			continue
		}

		tables = append(tables, MysqlTable{schema: schema, table: table})
	}

	return tables
}

func main() {
	flag.Parse()
	prometheus.MustRegister(NewMysqlCountCollector(*dsn))
	http.Handle(*path, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, please go to %q for metrics", html.EscapeString(*path))
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}

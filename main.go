package main

import (
	"database/sql"
	"flag"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"

	"github.com/go-sql-driver/mysql"
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
	rowCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, "row_count"),
		"Number of rows in the table",
		[]string{"schema", "table"},
		nil,
	)
)

var (
	addr        = flag.String("listen-address", ":9557", "The address to listen on for telemetry.")
	path        = flag.String("telemetry-path", "/metrics", "Path under which to expose metrics.")
	dsn         = flag.String("dsn", os.Getenv("DATA_SOURCE_NAME"), "A number of seconds to wait before re-counting rows")
	connections = flag.Int("max-connections", 1, "The maximum number of connections that will be opened to mysql")
	ignore      = flag.String("ignore", "", "Regex that matches table names to ignore")
)

func NewMysqlCountCollector(dataSourceName string, maxConnections int) *MysqlCountCollector {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		log.Fatalf("error connecting to database: %v", err)
	}
	db.SetMaxOpenConns(maxConnections)
	return &MysqlCountCollector{
		db: db,
		ScrapeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "scrape_errors",
			Help:      "Total number of times a mysql error occurred.",
		}, []string{"number"}),
		ignore: regexp.MustCompilePOSIX(*ignore),
	}
}

type MysqlCountCollector struct {
	db           *sql.DB
	ScrapeErrors *prometheus.CounterVec
	ignore       *regexp.Regexp
}

type MysqlTable struct {
	schema string
	table  string
}

func (c *MysqlCountCollector) Describe(ch chan<- *prometheus.Desc) {
	c.ScrapeErrors.Describe(ch)
	ch <- rowCountDesc
}

func (c *MysqlCountCollector) Collect(ch chan<- prometheus.Metric) {
	c.scrape(ch)
	c.ScrapeErrors.Collect(ch)
}

func (c *MysqlCountCollector) scrape(ch chan<- prometheus.Metric) {
	var wg sync.WaitGroup
	for _, t := range c.listTables() {
		wg.Add(1)
		go c.countTable(t.schema, t.table, ch, &wg)
	}
	wg.Wait()
}

func (c *MysqlCountCollector) countTable(schema, table string, ch chan<- prometheus.Metric, wg *sync.WaitGroup) {
	defer wg.Done()
	var count float64
	row := c.db.QueryRow("SELECT COUNT(*) FROM " + schema + "." + table)
	if err := row.Scan(&count); err != nil {
		log.Printf("error counting rows: %v", err)
		if mysqlerr, ok := err.(*mysql.MySQLError); ok {
			c.ScrapeErrors.WithLabelValues(strconv.FormatUint(uint64(mysqlerr.Number), 10)).Inc()
		}
		return
	}
	ch <- prometheus.MustNewConstMetric(rowCountDesc, prometheus.GaugeValue, count, schema, table)

}

func (c *MysqlCountCollector) listTables() []MysqlTable {
	var schema string
	var table string
	var tables []MysqlTable

	list, err := c.db.Query(listTablesQuery)
	if err != nil {
		log.Printf("error listing tables: %v", err)
		if mysqlerr, ok := err.(*mysql.MySQLError); ok {
			c.ScrapeErrors.WithLabelValues(strconv.FormatUint(uint64(mysqlerr.Number), 10)).Inc()
		}
		return tables
	}

	defer list.Close()

	for list.Next() {
		if err := list.Scan(
			&schema,
			&table,
		); err != nil {
			log.Printf("error listing tables: %v", err)
			continue
		}

		if c.ignore.Match([]byte(schema + "." + table)) {
			continue
		}

		tables = append(tables, MysqlTable{schema: schema, table: table})
	}

	return tables
}

func main() {
	flag.Parse()

	collector := NewMysqlCountCollector(*dsn, *connections)
	defer collector.db.Close()

	prometheus.MustRegister(collector)

	http.Handle(*path, promhttp.Handler())

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, please go to %q for metrics", html.EscapeString(*path))
	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}

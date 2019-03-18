package main

import (
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	rowCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "row_count",
			Help:      "Number of rows in the table",
		},
		[]string{
			"schema",
			"table",
		},
	)
	countTime = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "row_count_duration",
			Help:      "Time taken to count the rows in this table",
		},
		[]string{
			"schema",
			"table",
		},
	)
	tables = make(map[string]prometheus.Labels)
)

var (
	addr  = flag.String("listen-address", ":9557", "The address to listen on for telemetry.")
	path  = flag.String("telemetry-path", "/metrics", "Path under which to expose metrics.")
	pause = MustParseDuration(*flag.String("pause-duration", "10s", "A number of seconds to wait before re-counting rows"))
	dsn   = flag.String("dsn", os.Getenv("DATA_SOURCE_NAME"), "A number of seconds to wait before re-counting rows")
)

func Connect() (*sql.DB, error) {
	return sql.Open("mysql", *dsn)
}

func ListTables(db *sql.DB) error {
	rows, err := db.Query(listTablesQuery)
	if err != nil {
		return err
	}

	defer rows.Close()

	var schema string
	var table string

	for rows.Next() {
		if err := rows.Scan(
			&schema,
			&table,
		); err != nil {
			return err
		}
		tables[schema+"."+table] = prometheus.Labels{"schema": schema, "table": table}
	}
	return nil
}

func CountRows(db *sql.DB) error {
	for table, labels := range tables {
		timer := prometheus.NewTimer(prometheus.ObserverFunc(countTime.With(labels).Set))
		row := db.QueryRow("SELECT COUNT(*) FROM " + table)
		var count float64
		if err := row.Scan(&count); err != nil {
			if mysqlerr, ok := err.(*mysql.MySQLError); ok && mysqlerr.Number == 1146 {
				rowCount.Delete(labels)
				countTime.Delete(labels)
				delete(tables, table)
			} else {
				return err
			}
		} else {
			rowCount.With(labels).Set(count)
		}
		timer.ObserveDuration()
	}
	return nil
}

func ClearMetrics() {
	for table, labels := range tables {
		rowCount.Delete(labels)
		countTime.Delete(labels)
		delete(tables, table)
	}
}

func OnErr(message string, err error) {
	log.Printf("%s: %v", message, err)
	ClearMetrics()
	time.Sleep(pause)
}

func MustParseDuration(d string) time.Duration {
	duration, err := time.ParseDuration(d)
	if err != nil {
		log.Fatal(err)
	}
	return duration
}

func main() {
	db, err := Connect()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	go func() {
		for {
			if err = db.Ping(); err != nil {
				OnErr("Error connecting to database", err)
				continue
			}
			if err := ListTables(db); err != nil {
				OnErr("Error listing tables", err)
				continue
			}

			if err := CountRows(db); err != nil {
				OnErr("Error counting rows", err)
				continue
			}

			time.Sleep(pause)
		}
	}()

	http.Handle(*path, promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

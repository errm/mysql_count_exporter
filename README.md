# mysql_count_exporter

A prometheus exporter that exports the number of rows counted in each table
to `mysql_count_exporter_row_count`

A bit like the `mysql_info_schema_table_rows` metric exposed by https://github.com/prometheus/mysqld_exporter
except that rather than using the efficient, but inaccurate information_schema
does an inefficient but accurate `COUNT(*)` query against each table.

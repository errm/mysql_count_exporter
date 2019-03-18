FROM scratch
COPY mysql_count_exporter /
ENTRYPOINT ["/mysql_count_exporter"]

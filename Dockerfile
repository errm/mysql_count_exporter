FROM scratch
MAINTAINER Edward Robinson <edward-robinson@cookpad.com>

COPY mysql_count_exporter /

EXPOSE 9557
ENTRYPOINT ["/mysql_count_exporter"]

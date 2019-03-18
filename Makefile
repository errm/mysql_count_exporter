export GO111MODULE=on

GOCMD=go
BINARY_NAME=mysql_count_exporter

all: $(BINARY_NAME)

$(BINARY_NAME):
	$(GOCMD) build -ldflags="-s -w" -o $(BINARY_NAME) -v

upgrade:
	$(GOCMD) get -u

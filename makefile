default: all

dep:
	dep ensure

all: dep clean
	go build cmd/server/start_crawl.go
	go build cmd/crawl_update/server.go

clean:
	rm -f start_crawl server

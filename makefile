default: all

dep:
	dep ensure

all: dep clean
	go build cmd/start_crawl.go
	go build rest-api/server.go

clean:
	rm -f start_crawl server

default: all

dep:
	dep ensure

all: dep clean
	go build cmd/start_crawl.go
	go build cmd/server.go

clean:
	rm -f start_crawl server

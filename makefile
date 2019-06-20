default: all

dep:
	dep ensure

all: dep clean
	go build -o ./bin/crawl ./cmd/crawl/start_crawl.go
	go build -o ./bin/server ./cmd/server/server.go

clean:
	rm -f start_crawl server
	rm -rf bin/

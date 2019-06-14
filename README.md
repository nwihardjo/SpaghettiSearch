# SpaghettiSearch: A Concurrent Search Engine

Fully-functioning search engine built on top of Golang to satisfy HKUST COMP4321 requirements.

## Live Demo
http://vml1wk024.cse.ust.hk/ lmk if the link doesn't work.

## Features
- Combination of PageRank and vector-space model to rank the result
- Utilised anchor text and metatags suggested on [Google's paper](http://infolab.stanford.edu/pub/papers/google.pdf) to increase precision and index much more webpages
- Make use of generator, future, and fan-in fan-out concurrency pattern in Golang to increase retrieval performance
- Dynamic document summary retrieval 
- Use [BadgerDB](https://github.com/dgraph-io/badger) as database which optimised for SSD
- Support keyword list search and phrase search (use double quotes for phrase search)

## Setup & Installation

### Backend

- Install golang from [here](https://golang.org/doc/install)

```bash
$ sudo tar -C /usr/local -xzf go$VERSION.$OS-$ARCH.tar.gz
$ export PATH=$PATH:/usr/local/go/bin
```

- Download this repo using `go get`

```
$ go get github.com/nwihardjo/SpaghettiSearch
```

### Frontend

- Install node and npm from [here](https://www.npmjs.com/get-npm)

### Dependencies

[`dep`](https://golang.github.io/dep/) is used as the package management to ensure the installed dependencies are the correct version from the correct vendor. Run `dep ensure` on project root to install required packages, or run `go get ./...` to same thing.

### Building

- Run `make` in the project root directory. It will install the necessary binary packages as well as install dependendcies
- Run the crawler and specify the argument needed as below, then spin up the backend server
```bash
$ ./start_crawl [-numPages=<number of pages to be crawled>] [-startURL=<starting entry point for the crawler to crawl>] [-domainOnly=<whether webpages to be crawled only in the domain of given starting URL)]
$ ./server
```
- Finally, install and run the UI server by running command below. It will automatically redirect you to the port which the server is running
```bash
$ cd interface/
$ npm install
$ npm start
```

## Contributor
- [Nathaniel Wihardjo](https://github.com/nwihardjo)
- [Angelica Kosasih](https://github.com/ak2411)
- [Petra Gabriela](https://github.com/pgabriela)

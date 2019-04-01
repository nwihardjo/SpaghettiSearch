COMP4321 - Search Engine
Group 12

Guideline:
1. Install golang (Refer to https://golang.org/doc/install)
2. Put the project folder to $GOPATH/src/the-SearchEngine
3. Go to the project root directory and run "go build" command on terminal
4. Check whether `db_data/` and `docs/` directory exist in the project root directory. If it exists, delete `db_data/` and `docs/` using "rm -rf db_data/ docs/" command on terminal
4. To start the programme, run "./the-SearchEngine" on root directory
5. The output result can be found in project root directory as "spider_result.txt"

Files:
- The DB file which contain the indexed 30 pages is inside `db_data/` directory. Running "./theSearchEngine" after step 4 will re-generate the db file
- The output of the spider_result.txt is in project root directory

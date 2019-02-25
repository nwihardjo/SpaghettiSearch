package main


import (
	"fmt"
	"net/http"
	"io/ioutil"
	"os"
)

func main() {
	fmt.Println("Crawler started...")
	resp, err := http.Get("https://www.cse.ust.hk/")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Println(string(body[:len(body)]))
}

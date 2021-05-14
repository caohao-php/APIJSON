package main

import (
	"apijson/apijson"
	"context"
	"fmt"
	"io/ioutil"

	_ "github.com/go-sql-driver/mysql"

	stdhttp "net/http"
)

func HttpHandler(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	reqbody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		_, _ = fmt.Fprintln(w, "read body error")
	}

	ctx := context.Background()
	dbName := `root:apijson@tcp(apijson.cn:3306)/sys?timeout=1s&parseTime=true&charset=utf8&loc=Local`
	out, err := apijson.Parse(ctx, dbName, reqbody)
	_, _ = fmt.Fprintln(w, string(out))
}

func main() {
	addr := "127.0.0.1:8000"

	stdhttp.HandleFunc("/", HttpHandler)
	err := stdhttp.ListenAndServe(addr, nil)
	if err != nil {
		fmt.Printf("listen serve error: %v \n", err)
		return
	}

	HttpHandler(nil, nil)
}

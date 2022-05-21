package main

import (
	"github.com/VVoses/proglog/internal/server"
	"log"
)

func main() {
	srv := server.NewHTTPServer(":8090")
	log.Fatal(srv.ListenAndServe())
}

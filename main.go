package main

import (
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/rest"
	"log"
	"net/http"
)

func main() {
	server := rest.NewRestServer(eventstore.NewInMemoryStore(), 5)
	port := "8080"
	log.Printf("Starting http server on port %v\n", port)
	log.Fatal(http.ListenAndServe(":"+port, server))
}

package main

import (
	"database/sql"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/eventstore/mysql"
	"github.com/rieske/event-sourced-account-go/rest"
	"github.com/rieske/event-sourced-account-go/serialization"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	var eventStore eventsourcing.EventStore
	if mysqlURL, ok := os.LookupEnv("MYSQL_URL"); ok {
		db, err := sql.Open("mysql", mysqlURL)
		defer closeResource(db)
		if err != nil {
			log.Panic(err)
		}
		waitForDBConnection(db)
		mysql.MigrateSchema(db, "infrastructure/schema/mysql")
		sqlStore := mysql.NewEventStore(db)
		log.Println("Using mysql event store")
		eventStore = eventstore.NewSerializingEventStore(sqlStore, serialization.NewJsonEventSerializer())
	} else {
		log.Println("Using in-memory event store")
		eventStore = eventstore.NewInMemoryStore()
	}

	port := "8080"

	s := &http.Server{
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  20 * time.Second,
		Addr:         ":" + port,
		Handler:      rest.NewRestHandler(eventStore, 50),
	}
	s.SetKeepAlivesEnabled(false)
	log.Printf("Starting http server on port %v\n", port)
	log.Fatal(s.ListenAndServe())
}

func waitForDBConnection(db *sql.DB) {
	var err error
	for i := 0; i < 30; i++ {
		err = db.Ping()
		if err == nil {
			break
		}
		time.Sleep(time.Second * 1)
	}
	if err != nil {
		log.Panic(err)
	}
}

func closeResource(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Panic(err)
	}
}

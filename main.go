package main

import (
	"database/sql"
	_ "expvar"
	"fmt"
	zipkinsql "github.com/jcchavezs/zipkin-instrumentation-sql"
	"github.com/openzipkin/zipkin-go"
	zipkinhttp "github.com/openzipkin/zipkin-go/middleware/http"
	"github.com/openzipkin/zipkin-go/reporter"
	zipkinreporter "github.com/openzipkin/zipkin-go/reporter/http"
	"github.com/rieske/event-sourced-account-go/eventstore/postgres"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/eventstore/mysql"
	"github.com/rieske/event-sourced-account-go/rest"
	"github.com/rieske/event-sourced-account-go/serialization"
)

var (
	inUseConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "db_conn_in_use",
		Help: "Number of in-use database connections",
	})
	idleConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "db_conn_idle",
		Help: "Number of idle database connections",
	})
	openConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "db_conn_open",
		Help: "Number of open database connections",
	})
	maxOpenConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "db_conn_max",
		Help: "Number of max open database connections",
	})
)

func noTracingHttpHandler(h http.Handler) http.Handler {
	return h
}

func main() {
	var tracingHandler func(http.Handler) http.Handler
	var rep reporter.Reporter

	if zipkinURL, ok := os.LookupEnv("ZIPKIN_URL"); ok {
		rep = zipkinreporter.NewReporter(zipkinURL)
		defer closeResource(rep)
	}

	var eventStore eventsourcing.EventStore
	if postgresHost, ok := os.LookupEnv("POSTGRES_HOST"); ok {
		posrgresPort := requireEnvVariable("POSTGRES_PORT")
		posrgresUser := requireEnvVariable("POSTGRES_USER")
		posrgresPassword := requireEnvVariable("POSTGRES_PASSWORD")
		posrgresDB := requireEnvVariable("POSTGRES_DB")

		psqlInfo := fmt.Sprintf("host=%s port=%v user=%s password=%s dbname=%s sslmode=disable",
			postgresHost,
			posrgresPort,
			posrgresUser,
			posrgresPassword,
			posrgresDB,
		)
		driverName := "postgres"
		tracingHandler, driverName = buildTracingHandler(driverName, rep)

		db, err := sql.Open(driverName, psqlInfo)
		defer closeResource(db)
		if err != nil {
			log.Panic(err)
		}
		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(5)
		waitForDBConnection(db)
		postgres.MigrateSchema(db, "infrastructure/schema/postgres")

		dbMetrics(db)

		sqlStore := postgres.NewEventStore(db)
		log.Println("Using postgres event store")
		eventStore = eventstore.NewSerializingEventStore(sqlStore, serialization.NewMsgpackEventSerializer())
	} else if mysqlURL, ok := os.LookupEnv("MYSQL_URL"); ok {
		driverName := "mysql"
		tracingHandler, driverName = buildTracingHandler(driverName, rep)

		db, err := sql.Open(driverName, mysqlURL)
		defer closeResource(db)
		if err != nil {
			log.Panic(err)
		}
		db.SetMaxOpenConns(5)
		db.SetMaxIdleConns(5)
		waitForDBConnection(db)
		mysql.MigrateSchema(db, "infrastructure/schema/mysql")

		dbMetrics(db)

		sqlStore := mysql.NewEventStore(db)
		log.Println("Using mysql event store")
		eventStore = eventstore.NewSerializingEventStore(sqlStore, serialization.NewMsgpackEventSerializer())
	} else {
		log.Println("Using in-memory event store")
		eventStore = eventstore.NewInMemoryStore()
		tracingHandler = noTracingHttpHandler
	}

	shutdown := make(chan bool)
	http.Handle("/prometheus", promhttp.Handler())
	go func() {
		log.Print(http.ListenAndServe(":8081", nil))
		shutdown <- true
	}()

	servicePort := "8080"
	s := &http.Server{
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		IdleTimeout:  20 * time.Second,
		Addr:         ":" + servicePort,
		Handler:      tracingHandler(rest.NewRestHandler(eventStore, 50)),
	}
	go func() {
		log.Printf("Starting http server on port %v\n", servicePort)
		log.Print(s.ListenAndServe())
		shutdown <- true
	}()

	if _, ok := os.LookupEnv("CPU_PROFILE"); ok {
		go func() {
			http.ListenAndServe("0.0.0.0:6060", nil)
		}()
	}

	<-shutdown
}

func buildTracingHandler(driverName string, reporter reporter.Reporter) (func(http.Handler) http.Handler, string) {
	if reporter == nil {
		return noTracingHttpHandler, driverName
	}

	endpoint, err := zipkin.NewEndpoint("account-go", ":0")
	if err != nil {
		log.Fatalf("unable to create local endpoint: %v", err)
	}

	tracer, err := zipkin.NewTracer(reporter, zipkin.WithLocalEndpoint(endpoint))
	if err != nil {
		log.Fatalf("unable to create tracer: %v", err)
	}

	driverName, err = zipkinsql.Register(driverName, tracer, zipkinsql.WithAllTraceOptions(), zipkinsql.WithAllowRootSpan(false))
	if err != nil {
		log.Fatalf("unable to register zipkin driver: %v", err)
	}

	return zipkinhttp.NewServerMiddleware(tracer, zipkinhttp.TagResponseSize(true)), driverName
}

func requireEnvVariable(v string) string {
	if val, ok := os.LookupEnv(v); ok {
		return val
	}
	log.Fatalf("%s not specified", v)
	return ""
}

func dbMetrics(db *sql.DB) {
	go func() {
		for {
			s := db.Stats()
			inUseConnections.Set(float64(s.InUse))
			idleConnections.Set(float64(s.Idle))
			openConnections.Set(float64(s.OpenConnections))
			maxOpenConnections.Set(float64(s.MaxOpenConnections))
			time.Sleep(1 * time.Second)
		}
	}()
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

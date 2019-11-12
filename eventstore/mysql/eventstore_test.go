// +build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"os"
	"testing"
	"time"
)

var database *sql.DB
var store *sqlStore

func TestMain(m *testing.M) {
	ctx := context.Background()
	mysql := startMysqlContainer(ctx)
	defer terminateContainer(mysql, ctx)
	db, err := openDatabase(mysql, ctx)
	if err != nil {
		log.Panic(err)
	}
	defer CloseResource(db)
	database = db
	waitForMysqlContainerToStart()
	store = NewSqlStore(database)

	code := m.Run()

	os.Exit(code)
}

func terminateContainer(c testcontainers.Container, ctx context.Context) {
	err := c.Terminate(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func startMysqlContainer(ctx context.Context) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0.18",
		ExposedPorts: []string{"3306"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "test",
			"MYSQL_DATABASE":      "event_store",
			"MYSQL_USER":          "test",
			"MYSQL_PASSWORD":      "test",
		},
	}
	mysql, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Panic(err)
	}
	return mysql
}

func openDatabase(mysql testcontainers.Container, ctx context.Context) (*sql.DB, error) {
	port, err := mysql.MappedPort(ctx, "3306")
	if err != nil {
		log.Panic(err)
	}

	return sql.Open("mysql", fmt.Sprintf("test:test@tcp(127.0.0.1:%v)/event_store", port.Port()))
}

func waitForMysqlContainerToStart() {
	var err error
	for i := 0; i < 30; i++ {
		err = database.Ping()
		if err == nil {
			break
		}
		time.Sleep(time.Second * 1)
	}
	if err != nil {
		log.Panic(err)
	}
}

func TestSqlStore_Events_Empty(t *testing.T) {
	events := store.Events(account.NewAccountId(), 0)

	assert.Empty(t, events)
}

func TestSqlStore_Events_SingleEvent(t *testing.T) {

	id := account.NewAccountId()
	err := store.Append([]eventstore.SequencedEvent{{
		AggregateId: id,
		Seq:         1,
		Event:       nil,
	}}, nil, uuid.New())
	assert.NoError(t, err)

	events := store.Events(id, 0)

	assert.NotEmpty(t, events)
}

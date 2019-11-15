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
	events, err := store.Events(account.NewAccountId(), 0)

	assert.NoError(t, err)
	assert.Empty(t, events)
}

func TestSqlStore_Events_SingleEvent(t *testing.T) {
	id := account.NewAccountId()
	expectedEvents := []eventstore.SerializedEvent{{
		AggregateId: id,
		Seq:         11,
		Payload:     []byte("test"),
		EventType:   42,
	}}
	err := store.Append(expectedEvents, nil, uuid.New())
	assert.NoError(t, err)

	events, err := store.Events(id, 0)

	assert.NoError(t, err)
	assert.Equal(t, expectedEvents, events)
}

func TestSqlStore_NoTransactionExists(t *testing.T) {
	transactionExists, err := store.TransactionExists(account.NewAccountId(), uuid.New())

	assert.NoError(t, err)
	assert.False(t, transactionExists)
}

func TestSqlStore_NoSnapshot(t *testing.T) {
	event, err := store.LoadSnapshot(account.NewAccountId())

	assert.NoError(t, err)
	assert.Nil(t, event)
}

func TestSqlStore_InsertTransactionIdForAllAggregatesInEvents(t *testing.T) {
	sourceAccount := account.NewAccountId()
	targetAccount := account.NewAccountId()
	expectedEvents := []eventstore.SerializedEvent{
		{
			AggregateId: sourceAccount,
			Seq:         1,
			Payload:     []byte("test1"),
			EventType:   2,
		},
		{
			AggregateId: targetAccount,
			Seq:         1,
			Payload:     []byte("test2"),
			EventType:   2,
		},
	}
	txId := uuid.New()
	err := store.Append(expectedEvents, nil, txId)
	assert.NoError(t, err)

	transactionExists, err := store.TransactionExists(sourceAccount, txId)
	assert.NoError(t, err)
	assert.True(t, transactionExists)
	transactionExists, err = store.TransactionExists(targetAccount, txId)
	assert.NoError(t, err)
	assert.True(t, transactionExists)
	transactionExists, err = store.TransactionExists(account.NewAccountId(), txId)
	assert.NoError(t, err)
	assert.False(t, transactionExists)
}

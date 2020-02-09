// +build integration

package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/eventstore/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var store *mysql.EventStore

func TestMain(m *testing.M) {
	ctx := context.Background()
	mysqlContainer := startMysqlContainer(ctx)
	db, err := openDatabase(mysqlContainer, ctx)
	if err != nil {
		log.Panic(err)
	}
	mysql.MigrateSchema(db, "../../infrastructure/schema/mysql")
	store = mysql.NewEventStore(db)

	code := m.Run()

	closeResource(db)
	terminateContainer(mysqlContainer, ctx)

	os.Exit(code)
}

func terminateContainer(c testcontainers.Container, ctx context.Context) {
	log.Println("Terminating mysql container")
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
		Tmpfs:      map[string]string{"/var/lib/mysql": "rw"},
		WaitingFor: wait.ForLog("port: 3306"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Panic(err)
	}

	log.Println("Started mysql container")
	return container
}

func openDatabase(mysql testcontainers.Container, ctx context.Context) (*sql.DB, error) {
	port, err := mysql.MappedPort(ctx, "3306")
	if err != nil {
		log.Panic(err)
	}

	return sql.Open("mysql", fmt.Sprintf("test:test@tcp(127.0.0.1:%v)/event_store", port.Port()))
}

func closeResource(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Panic(err)
	}
}

func TestSqlStore_Events_Empty(t *testing.T) {
	events, err := store.Events(context.Background(), account.NewID(), 0)

	assert.NoError(t, err)
	assert.Empty(t, events)
}

func TestSqlStore_Events_SingleEvent(t *testing.T) {
	id := account.NewID()
	expectedEvents := []eventstore.SerializedEvent{{
		AggregateId: id,
		Seq:         11,
		Payload:     []byte("test"),
		EventType:   42,
	}}
	err := store.Append(context.Background(), expectedEvents, nil, uuid.New())
	assert.NoError(t, err)

	events, err := store.Events(context.Background(), id, 0)

	assert.NoError(t, err)
	assert.Equal(t, expectedEvents, events)
}

func TestSqlStore_NoTransactionExists(t *testing.T) {
	transactionExists, err := store.TransactionExists(context.Background(), account.NewID(), uuid.New())

	assert.NoError(t, err)
	assert.False(t, transactionExists)
}

func TestSqlStore_NoSnapshot(t *testing.T) {
	event, err := store.LoadSnapshot(context.Background(), account.NewID())

	assert.NoError(t, err)
	assert.Nil(t, event)
}

func TestSqlStore_InsertTransactionIdForAllAggregatesInEvents(t *testing.T) {
	sourceAccount := account.NewID()
	targetAccount := account.NewID()
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
	err := store.Append(context.Background(), expectedEvents, nil, txId)
	assert.NoError(t, err)

	transactionExists, err := store.TransactionExists(context.Background(), sourceAccount, txId)
	assert.NoError(t, err)
	assert.True(t, transactionExists)
	transactionExists, err = store.TransactionExists(context.Background(), targetAccount, txId)
	assert.NoError(t, err)
	assert.True(t, transactionExists)
	transactionExists, err = store.TransactionExists(context.Background(), account.NewID(), txId)
	assert.NoError(t, err)
	assert.False(t, transactionExists)
}

func TestSqlStore_Snapshot(t *testing.T) {
	id := account.NewID()
	expectedSnapshot := eventstore.SerializedEvent{
		AggregateId: id,
		Seq:         11,
		Payload:     []byte("test"),
		EventType:   42,
	}
	err := store.Append(context.Background(), []eventstore.SerializedEvent{}, map[account.ID]eventstore.SerializedEvent{id: expectedSnapshot}, uuid.New())
	assert.NoError(t, err)

	snapshot, err := store.LoadSnapshot(context.Background(), id)

	assert.NoError(t, err)
	assert.NotNil(t, snapshot)
	assert.Equal(t, expectedSnapshot, *snapshot)
}

func TestSqlStore_ConcurrentModificationErrorOnDuplicateEventSequence(t *testing.T) {
	id := account.NewID()
	expectedEvents := []eventstore.SerializedEvent{{
		AggregateId: id,
		Seq:         11,
		Payload:     []byte("test"),
		EventType:   42,
	}}
	err := store.Append(context.Background(), expectedEvents, nil, uuid.New())
	assert.NoError(t, err)

	duplicateSequence := []eventstore.SerializedEvent{{
		AggregateId: id,
		Seq:         11,
		Payload:     []byte("banana"),
		EventType:   10,
	}}
	err = store.Append(context.Background(), duplicateSequence, nil, uuid.New())
	assert.Equal(t, err, account.ConcurrentModification)

	events, err := store.Events(context.Background(), id, 0)

	assert.NoError(t, err)
	assert.Equal(t, expectedEvents, events)
}

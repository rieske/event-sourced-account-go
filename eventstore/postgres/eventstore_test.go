// +build integration

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/eventstore/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

var store *postgres.EventStore

func TestMain(m *testing.M) {
	ctx := context.Background()
	postgresContainer := startPostgresContainer(ctx)
	db, err := openDatabase(postgresContainer, ctx)
	if err != nil {
		log.Panic(err)
	}
	if err := db.Ping(); err != nil {
		log.Panic(err)
	}
	postgres.MigrateSchema(db, "../../infrastructure/schema/postgres")
	store = postgres.NewEventStore(db)

	code := m.Run()

	closeResource(db)
	terminateContainer(postgresContainer, ctx)

	os.Exit(code)
}

func terminateContainer(c testcontainers.Container, ctx context.Context) {
	log.Println("Terminating postgres container")
	err := c.Terminate(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func startPostgresContainer(ctx context.Context) testcontainers.Container {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:12.2",
		ExposedPorts: []string{"5432"},
		Env: map[string]string{
			"POSTGRES_DB":       "event_store",
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
		},
		Tmpfs:      map[string]string{"/var/lib/postgresql/data": "rw"},
		WaitingFor: wait.ForLog("[1] LOG:  database system is ready to accept connections"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Panic(err)
	}

	log.Println("Started postgres container")
	return container
}

func openDatabase(postgres testcontainers.Container, ctx context.Context) (*sql.DB, error) {
	port, err := postgres.MappedPort(ctx, "5432")
	if err != nil {
		log.Panic(err)
	}

	psqlInfo := fmt.Sprintf("host=127.0.0.1 port=%v user=test password=test dbname=event_store sslmode=disable", port.Port())

	return sql.Open("postgres", psqlInfo)
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
	err := store.Append(context.Background(), []eventstore.SerializedEvent{}, []eventstore.SerializedEvent{expectedSnapshot}, uuid.New())
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
	assert.Equal(t, account.ConcurrentModification, err)

	events, err := store.Events(context.Background(), id, 0)

	assert.NoError(t, err)
	assert.Equal(t, expectedEvents, events)
}

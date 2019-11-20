// +build integration

package mysql_test

import (
	"database/sql"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/eventstore/mysql"
	"github.com/rieske/event-sourced-account-go/test"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var store *mysql.EventStore

func TestMain(m *testing.M) {
	test.WithMysqlDatabase(func(db *sql.DB) {
		mysql.MigrateSchema(db, "../../infrastructure/schema/mysql")
		store = mysql.NewEventStore(db)

		os.Exit(m.Run())
	})
}

func TestSqlStore_Events_Empty(t *testing.T) {
	events, err := store.Events(account.NewID(), 0)

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
	err := store.Append(expectedEvents, nil, uuid.New())
	assert.NoError(t, err)

	events, err := store.Events(id, 0)

	assert.NoError(t, err)
	assert.Equal(t, expectedEvents, events)
}

func TestSqlStore_NoTransactionExists(t *testing.T) {
	transactionExists, err := store.TransactionExists(account.NewID(), uuid.New())

	assert.NoError(t, err)
	assert.False(t, transactionExists)
}

func TestSqlStore_NoSnapshot(t *testing.T) {
	event, err := store.LoadSnapshot(account.NewID())

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
	err := store.Append(expectedEvents, nil, txId)
	assert.NoError(t, err)

	transactionExists, err := store.TransactionExists(sourceAccount, txId)
	assert.NoError(t, err)
	assert.True(t, transactionExists)
	transactionExists, err = store.TransactionExists(targetAccount, txId)
	assert.NoError(t, err)
	assert.True(t, transactionExists)
	transactionExists, err = store.TransactionExists(account.NewID(), txId)
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
	err := store.Append([]eventstore.SerializedEvent{}, map[account.ID]eventstore.SerializedEvent{id: expectedSnapshot}, uuid.New())
	assert.NoError(t, err)

	snapshot, err := store.LoadSnapshot(id)

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
	err := store.Append(expectedEvents, nil, uuid.New())
	assert.NoError(t, err)

	duplicateSequence := []eventstore.SerializedEvent{{
		AggregateId: id,
		Seq:         11,
		Payload:     []byte("banana"),
		EventType:   10,
	}}
	err = store.Append(duplicateSequence, nil, uuid.New())
	assert.Equal(t, err, account.ConcurrentModification)

	events, err := store.Events(id, 0)

	assert.NoError(t, err)
	assert.Equal(t, expectedEvents, events)
}

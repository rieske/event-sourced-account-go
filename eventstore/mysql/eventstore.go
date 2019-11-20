package mysql

import (
	"database/sql"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"io"
	"log"
	"strings"
)

type EventStore struct {
	db *sql.DB
}

const (
	appendEventSql  = "INSERT INTO event_store.Event(aggregateId, sequenceNumber, transactionId, eventType, payload) VALUES(?, ?, ?, ?, ?)"
	selectEventsSql = "SELECT sequenceNumber, eventType, payload FROM event_store.Event WHERE aggregateId = ? AND sequenceNumber > ? ORDER BY sequenceNumber ASC"

	removeSnapshotSql = "DELETE FROM event_store.Snapshot WHERE aggregateId = ?"
	storeSnapshotSql  = "INSERT INTO event_store.Snapshot(aggregateId, sequenceNumber, eventType, payload) VALUES(?, ?, ?, ?)"
	selectSnapshotSql = "SELECT sequenceNumber, eventType, payload FROM event_store.Snapshot WHERE aggregateId = ?"

	insertTransactionSql = "INSERT INTO event_store.Transaction(aggregateId, transactionId) VALUES(?, ?)"
	selectTransactionSql = "SELECT aggregateId FROM event_store.Transaction WHERE aggregateId = ? AND transactionId = ?"
)

func MigrateSchema(db *sql.DB, schemaLocation string) {
	driver, err := mysql.WithInstance(db, &mysql.Config{})
	if err != nil {
		log.Panic(err)
	}
	m, err := migrate.NewWithDatabaseInstance("file://"+schemaLocation, "event_store", driver)
	if err != nil {
		log.Panic(err)
	}

	if err := m.Migrate(3); err != nil && err != migrate.ErrNoChange {
		log.Panic(err)
	}
}

func NewEventStore(db *sql.DB) *EventStore {
	return &EventStore{db: db}
}

func (es *EventStore) sqlSelect(
	selectSql string,
	query func(stmt *sql.Stmt) (*sql.Rows, error),
	rowExtractor func(rows *sql.Rows) error,
) error {
	stmt, err := es.db.Prepare(selectSql)
	if err != nil {
		return err
	}
	defer closeResource(stmt)
	rows, err := query(stmt)
	if err != nil {
		return err
	}
	defer closeResource(rows)

	if err := rowExtractor(rows); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (es *EventStore) Events(id account.ID, version int) ([]eventstore.SerializedEvent, error) {
	var events []eventstore.SerializedEvent

	err := es.sqlSelect(
		selectEventsSql,
		func(stmt *sql.Stmt) (*sql.Rows, error) {
			return stmt.Query(id.UUID[:], version)
		},
		func(rows *sql.Rows) error {
			for rows.Next() {
				event := eventstore.SerializedEvent{AggregateId: id}
				err := rows.Scan(&event.Seq, &event.EventType, &event.Payload)
				if err != nil {
					return err
				}
				events = append(events, event)
			}
			return nil
		},
	)

	return events, err
}

func (es *EventStore) LoadSnapshot(id account.ID) (*eventstore.SerializedEvent, error) {
	var snapshot *eventstore.SerializedEvent

	err := es.sqlSelect(
		selectSnapshotSql,
		func(stmt *sql.Stmt) (*sql.Rows, error) {
			return stmt.Query(id.UUID[:])
		},
		func(rows *sql.Rows) error {
			if rows.Next() {
				event := eventstore.SerializedEvent{AggregateId: id}
				err := rows.Scan(&event.Seq, &event.EventType, &event.Payload)
				if err != nil {
					return err
				}
				snapshot = &event
			}
			return nil
		},
	)

	return snapshot, err
}

func (es *EventStore) TransactionExists(id account.ID, txId uuid.UUID) (bool, error) {
	transactionExists := false

	err := es.sqlSelect(
		selectTransactionSql,
		func(stmt *sql.Stmt) (*sql.Rows, error) {
			return stmt.Query(id.UUID[:], txId[:])
		},
		func(rows *sql.Rows) error {
			transactionExists = rows.Next()
			return nil
		},
	)

	return transactionExists, err
}

func (es *EventStore) Append(events []eventstore.SerializedEvent, snapshots map[account.ID]eventstore.SerializedEvent, txId uuid.UUID) error {
	if err := es.append(events, snapshots, txId); err != nil {
		return toConcurrentModification(err)
	}
	return nil
}

func (es *EventStore) append(events []eventstore.SerializedEvent, snapshots map[account.ID]eventstore.SerializedEvent, txId uuid.UUID) error {
	return es.withTransaction(func(tx *sql.Tx) error {
		if err := insertTransaction(tx, events, txId); err != nil {
			return err
		}
		if err := insertEvents(tx, events, txId); err != nil {
			return err
		}
		if err := updateSnapshots(tx, snapshots); err != nil {
			return err
		}
		return nil
	})
}

func (es *EventStore) withTransaction(doInTx func(tx *sql.Tx) error) error {
	tx, err := es.db.Begin()
	if err != nil {
		return err
	}

	if err := doInTx(tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("error while rolling back tx %v, original error %v", rollbackErr, err)
		}
		return err
	}

	return tx.Commit()
}

func insertTransaction(tx *sql.Tx, events []eventstore.SerializedEvent, txId uuid.UUID) error {
	insertTransactionsStmt, err := tx.Prepare(insertTransactionSql)
	if err != nil {
		return err
	}
	defer closeResource(insertTransactionsStmt)

	aggregateIds := map[account.ID]bool{}
	for _, event := range events {
		aggregateIds[event.AggregateId] = true
	}
	for aggregateId := range aggregateIds {
		if _, err := insertTransactionsStmt.Exec(aggregateId.UUID[:], txId[:]); err != nil {
			return err
		}
	}
	return nil
}

func insertEvents(tx *sql.Tx, events []eventstore.SerializedEvent, txId uuid.UUID) error {
	insertEventsStmt, err := tx.Prepare(appendEventSql)
	if err != nil {
		return err
	}
	defer closeResource(insertEventsStmt)

	for _, event := range events {
		if _, err := insertEventsStmt.Exec(event.AggregateId.UUID[:], event.Seq, txId[:], event.EventType, event.Payload); err != nil {
			return err
		}
	}
	return nil
}

func updateSnapshots(tx *sql.Tx, snapshots map[account.ID]eventstore.SerializedEvent) error {
	deleteSnapshotsStmt, err := tx.Prepare(removeSnapshotSql)
	if err != nil {
		return err
	}
	defer closeResource(deleteSnapshotsStmt)

	insertSnapshotsStmt, err := tx.Prepare(storeSnapshotSql)
	if err != nil {
		return err
	}
	defer closeResource(insertSnapshotsStmt)
	for aggregateId, snapshot := range snapshots {
		if _, err := deleteSnapshotsStmt.Exec(aggregateId.UUID[:]); err != nil {
			return err
		}
		if _, err := insertSnapshotsStmt.Exec(snapshot.AggregateId.UUID[:], snapshot.Seq, snapshot.EventType, snapshot.Payload); err != nil {
			return err
		}
	}
	return nil
}

func toConcurrentModification(err error) error {
	if strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry") && strings.HasSuffix(err.Error(), "for key 'PRIMARY'") {
		return account.ConcurrentModification
	} else if err.Error() == "Error 1213: Deadlock found when trying to get lock; try restarting transaction" {
		return account.ConcurrentModification
	} else {
		return err
	}
}

func closeResource(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("Could not close resource: %v\n", err)
	}
}

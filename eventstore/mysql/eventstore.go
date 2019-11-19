package mysql

import (
	"database/sql"
	"errors"
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

	if err := m.Steps(3); err != nil {
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
	defer CloseResource(stmt)
	rows, err := query(stmt)
	if err != nil {
		return err
	}
	defer CloseResource(rows)

	if err := rowExtractor(rows); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (es *EventStore) Events(id account.Id, version int) ([]eventstore.SerializedEvent, error) {
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

func (es *EventStore) LoadSnapshot(id account.Id) (*eventstore.SerializedEvent, error) {
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

func (es *EventStore) TransactionExists(id account.Id, txId uuid.UUID) (bool, error) {
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

func (es *EventStore) Append(events []eventstore.SerializedEvent, snapshots map[account.Id]eventstore.SerializedEvent, txId uuid.UUID) error {
	err := es.append(events, snapshots, txId)
	return toConcurrentModification(err)
}

func (es *EventStore) append(events []eventstore.SerializedEvent, snapshots map[account.Id]eventstore.SerializedEvent, txId uuid.UUID) error {
	aggregateIds := map[account.Id]bool{}
	for _, event := range events {
		aggregateIds[event.AggregateId] = true
	}

	tx, err := es.db.Begin()
	if err != nil {
		return err
	}

	insertTransactionsStmt, err := tx.Prepare(insertTransactionSql)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer CloseResource(insertTransactionsStmt)

	for aggregateId, _ := range aggregateIds {
		_, err := insertTransactionsStmt.Exec(aggregateId.UUID[:], txId[:])
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	insertEventsStmt, err := tx.Prepare(appendEventSql)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer CloseResource(insertEventsStmt)

	for _, event := range events {
		_, err := insertEventsStmt.Exec(event.AggregateId.UUID[:], event.Seq, txId[:], event.EventType, event.Payload)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	deleteSnapshotsStmt, err := tx.Prepare(removeSnapshotSql)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer CloseResource(deleteSnapshotsStmt)
	insertSnapshotsStmt, err := tx.Prepare(storeSnapshotSql)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer CloseResource(insertSnapshotsStmt)
	for aggregateId, snapshot := range snapshots {
		_, err := deleteSnapshotsStmt.Exec(aggregateId.UUID[:])
		if err != nil {
			tx.Rollback()
			return err
		}
		_, err = insertSnapshotsStmt.Exec(snapshot.AggregateId.UUID[:], snapshot.Seq, snapshot.EventType, snapshot.Payload)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func toConcurrentModification(err error) error {
	if err == nil {
		return nil
	}
	if strings.HasPrefix(err.Error(), "Error 1062: Duplicate entry") &&
		strings.HasSuffix(err.Error(), "for key 'PRIMARY'") {
		return errors.New("concurrent modification error")
	} else if err.Error() == "Error 1213: Deadlock found when trying to get lock; try restarting transaction" {
		return errors.New("concurrent modification error")
	} else {
		return err
	}
}

func CloseResource(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Println(err)
	}
}

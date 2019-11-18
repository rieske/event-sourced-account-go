package mysql

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
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

func NewEventStore(db *sql.DB, schemaLocation string) *EventStore {
	driver, err := mysql.WithInstance(db, &mysql.Config{})
	if err != nil {
		log.Panic(err)
	}
	m, err := migrate.NewWithDatabaseInstance("file://"+schemaLocation, "event_store", driver)
	if err != nil {
		log.Panic(err)
	}
	err = m.Steps(3)
	if err != nil {
		log.Panic(err)
	}
	return &EventStore{db: db}
}

func (es *EventStore) Events(id account.Id, version int) ([]eventstore.SerializedEvent, error) {
	stmt, err := es.db.Prepare("SELECT sequenceNumber, eventType, payload FROM event_store.Event WHERE aggregateId = ? AND sequenceNumber > ? ORDER BY sequenceNumber ASC")
	if err != nil {
		return nil, err
	}
	defer CloseResource(stmt)
	rows, err := stmt.Query(id.UUID[:], version)
	if err != nil {
		return nil, err
	}
	defer CloseResource(rows)

	var events []eventstore.SerializedEvent
	for rows.Next() {
		event := eventstore.SerializedEvent{AggregateId: id}
		err := rows.Scan(&event.Seq, &event.EventType, &event.Payload)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (es *EventStore) LoadSnapshot(id account.Id) (*eventstore.SerializedEvent, error) {
	stmt, err := es.db.Prepare("SELECT sequenceNumber, eventType, payload FROM event_store.Snapshot WHERE aggregateId = ?")
	if err != nil {
		return nil, err
	}
	defer CloseResource(stmt)
	rows, err := stmt.Query(id.UUID[:])
	if err != nil {
		return nil, err
	}
	defer CloseResource(rows)

	var snapshot *eventstore.SerializedEvent
	if rows.Next() {
		event := eventstore.SerializedEvent{AggregateId: id}
		err := rows.Scan(&event.Seq, &event.EventType, &event.Payload)
		if err != nil {
			return nil, err
		}
		snapshot = &event
	}
	if err = rows.Err(); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (es *EventStore) TransactionExists(id account.Id, txId uuid.UUID) (bool, error) {
	stmt, err := es.db.Prepare("SELECT aggregateId FROM event_store.Transaction WHERE aggregateId = ? AND transactionId = ?")
	if err != nil {
		return false, err
	}
	defer CloseResource(stmt)
	rows, err := stmt.Query(id.UUID[:], txId[:])
	if err != nil {
		return false, err
	}
	defer CloseResource(rows)
	transactionExists := rows.Next()
	if err = rows.Err(); err != nil {
		return transactionExists, err
	}

	return transactionExists, nil
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

	insertTransactionsStmt, err := tx.Prepare("INSERT INTO event_store.Transaction(aggregateId, transactionId) VALUES(?, ?)")
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

	insertEventsStmt, err := tx.Prepare("INSERT INTO event_store.Event(aggregateId, sequenceNumber, transactionId, eventType, payload) VALUES(?, ?, ?, ?, ?)")
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

	deleteSnapshotsStmt, err := tx.Prepare("DELETE FROM event_store.Snapshot WHERE aggregateId = ?")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer CloseResource(deleteSnapshotsStmt)
	insertSnapshotsStmt, err := tx.Prepare("INSERT INTO event_store.Snapshot(aggregateId, sequenceNumber, eventType, payload) VALUES(?, ?, ?, ?)")
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
	err := c.Close()
	if err != nil {
		log.Println(err)
	}
}

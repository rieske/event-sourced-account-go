package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"io"
	"log"
)

type sqlStore struct {
	db *sql.DB
}

func NewSqlStore(db *sql.DB) *sqlStore {
	driver, err := mysql.WithInstance(db, &mysql.Config{})
	if err != nil {
		log.Panic(err)
	}
	m, err := migrate.NewWithDatabaseInstance("file://schema", "event_store", driver)
	if err != nil {
		log.Panic(err)
	}
	err = m.Steps(3)
	if err != nil {
		log.Panic(err)
	}
	return &sqlStore{db: db}
}

func (es *sqlStore) Events(id account.Id, version int) []eventstore.SequencedEvent {
	//stmt, err := es.db.Prepare("SELECT sequenceNumber, transactionId, payload FROM event_store.Event WHERE aggregateId = ? AND sequenceNumber > ? ORDER BY sequenceNumber ASC")
	stmt, err := es.db.Prepare("SELECT sequenceNumber FROM event_store.Event WHERE aggregateId = ? AND sequenceNumber > ? ORDER BY sequenceNumber ASC")
	if err != nil {
		log.Panic(err)
	}
	defer CloseResource(stmt)
	rows, err := stmt.Query(id[:], version)
	if err != nil {
		log.Panic(err)
	}
	defer CloseResource(rows)

	var events []eventstore.SequencedEvent
	for rows.Next() {
		event := eventstore.SequencedEvent{AggregateId: id}
		err := rows.Scan(&event.Seq)
		if err != nil {
			log.Panic(err)
		}
		events = append(events, event)
	}
	if err = rows.Err(); err != nil {
		log.Panic(err)
	}
	return events
}

func (es *sqlStore) LoadSnapshot(id account.Id) eventstore.SequencedEvent {
	return eventstore.SequencedEvent{}
}

func (es *sqlStore) TransactionExists(id account.Id, txId uuid.UUID) bool {
	return false
}

func (es *sqlStore) Append(events []eventstore.SequencedEvent, snapshots map[account.Id]eventstore.SequencedEvent, txId uuid.UUID) error {
	tx, err := es.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := es.db.Prepare("INSERT INTO event_store.Event(aggregateId, sequenceNumber, transactionId, payload) VALUES(?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer CloseResource(stmt)

	event := events[0]
	_, err = stmt.Exec(event.AggregateId[:], event.Seq, txId[:], "aaa")
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func CloseResource(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Panic(err)
	}
}

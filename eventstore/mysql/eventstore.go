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

func (es *sqlStore) Events(id account.Id, version int) ([]*eventstore.SerializedEvent, error) {
	//stmt, err := es.db.Prepare("SELECT sequenceNumber, transactionId, payload FROM event_store.Event WHERE aggregateId = ? AND sequenceNumber > ? ORDER BY sequenceNumber ASC")
	stmt, err := es.db.Prepare("SELECT sequenceNumber FROM event_store.Event WHERE aggregateId = ? AND sequenceNumber > ? ORDER BY sequenceNumber ASC")
	if err != nil {
		return nil, err
	}
	defer CloseResource(stmt)
	rows, err := stmt.Query(id.UUID[:], version)
	if err != nil {
		return nil, err
	}
	defer CloseResource(rows)

	var events []*eventstore.SerializedEvent
	for rows.Next() {
		event := eventstore.SerializedEvent{AggregateId: id}
		err := rows.Scan(&event.Seq)
		if err != nil {
			return nil, err
		}
		events = append(events, &event)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func (es *sqlStore) LoadSnapshot(id account.Id) (*eventstore.SerializedEvent, error) {
	return &eventstore.SerializedEvent{}, nil
}

func (es *sqlStore) TransactionExists(id account.Id, txId uuid.UUID) (bool, error) {
	return false, nil
}

func (es *sqlStore) Append(events []*eventstore.SerializedEvent, snapshots map[account.Id]*eventstore.SerializedEvent, txId uuid.UUID) error {
	tx, err := es.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO event_store.Event(aggregateId, sequenceNumber, transactionId, payload) VALUES(?, ?, ?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer CloseResource(stmt)

	for _, event := range events {
		_, err = stmt.Exec(event.AggregateId.UUID[:], event.Seq, txId[:], "aaa")
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func CloseResource(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Panic(err)
	}
}

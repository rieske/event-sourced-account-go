package eventstore

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"io"
	"log"
)

type sqlStore struct {
	db *sql.DB
}

func NewSqlStore(db *sql.DB) *sqlStore {
	return &sqlStore{db: db}
}

func (es *sqlStore) Events(id account.Id, version int) []SequencedEvent {
	stmt, err := es.db.Prepare("SELECT sequenceNumber, transactionId, payload FROM event_store.Event WHERE aggregateId = ? AND sequenceNumber > ? ORDER BY sequenceNumber ASC")
	if err != nil {
		log.Panic(err)
	}
	defer CloseResource(stmt)
	return nil
}

func (es *sqlStore) LoadSnapshot(id account.Id) SequencedEvent {
	return SequencedEvent{}
}

func (es *sqlStore) TransactionExists(id account.Id, txId uuid.UUID) bool {
	return false
}

func (es *sqlStore) Append(events []SequencedEvent, snapshots map[account.Id]SequencedEvent, txId uuid.UUID) error {
	return nil
}

func CloseResource(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Fatal(err)
	}
}

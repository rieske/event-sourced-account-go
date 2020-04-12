package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
)

type EventStore struct {
	db                    *sql.DB
	selectEventsStmt      *sql.Stmt
	selectSnapshotStmt    *sql.Stmt
	removeSnapshotStmt    *sql.Stmt
	selectTransactionStmt *sql.Stmt
	storeSnapshotStmt     *sql.Stmt
	insertTransactionStmt *sql.Stmt
	appendEventStmt       *sql.Stmt
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
	return &EventStore{
		db:                    db,
		selectEventsStmt:      prepareStatementOrPanic(db, selectEventsSql),
		selectSnapshotStmt:    prepareStatementOrPanic(db, selectSnapshotSql),
		selectTransactionStmt: prepareStatementOrPanic(db, selectTransactionSql),
		removeSnapshotStmt:    prepareStatementOrPanic(db, removeSnapshotSql),
		storeSnapshotStmt:     prepareStatementOrPanic(db, storeSnapshotSql),
		insertTransactionStmt: prepareStatementOrPanic(db, insertTransactionSql),
		appendEventStmt:       prepareStatementOrPanic(db, appendEventSql),
	}
}

func prepareStatementOrPanic(db *sql.DB, sql string) *sql.Stmt {
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic(err)
	}
	return stmt
}

func (es EventStore) Events(ctx context.Context, id account.ID, version int) ([]eventstore.SerializedEvent, error) {
	var events []eventstore.SerializedEvent

	err := sqlSelect(
		ctx,
		es.selectEventsStmt,
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
		id.UUID[:], version,
	)

	return events, err
}

func (es EventStore) LoadSnapshot(ctx context.Context, id account.ID) (*eventstore.SerializedEvent, error) {
	var snapshot *eventstore.SerializedEvent

	err := sqlSelect(
		ctx,
		es.selectSnapshotStmt,
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
		id.UUID[:],
	)

	return snapshot, err
}

func (es EventStore) TransactionExists(ctx context.Context, id account.ID, txId uuid.UUID) (bool, error) {
	transactionExists := false

	err := sqlSelect(
		ctx,
		es.selectTransactionStmt,
		func(rows *sql.Rows) error {
			transactionExists = rows.Next()
			return nil
		},
		id.UUID[:], txId[:],
	)

	return transactionExists, err
}

func (es EventStore) Append(ctx context.Context, events []eventstore.SerializedEvent, snapshots []eventstore.SerializedEvent, txId uuid.UUID) error {
	if err := es.append(ctx, events, snapshots, txId); err != nil {
		return toConcurrentModification(err)
	}
	return nil
}

func (es EventStore) append(ctx context.Context, events []eventstore.SerializedEvent, snapshots []eventstore.SerializedEvent, txId uuid.UUID) error {
	return es.withTransaction(ctx, func(tx *sql.Tx) error {
		if err := es.insertEvents(ctx, tx, events, txId); err != nil {
			return err
		}
		if err := es.insertTransaction(ctx, tx, events, txId); err != nil {
			return err
		}
		if len(snapshots) != 0 {
			if err := es.updateSnapshots(ctx, tx, snapshots); err != nil {
				return err
			}
		}
		return nil
	})
}

func (es EventStore) withTransaction(ctx context.Context, doInTx func(tx *sql.Tx) error) error {
	tx, err := es.db.BeginTx(ctx, nil)
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

func sqlSelect(
	ctx context.Context,
	stmt *sql.Stmt,
	rowExtractor func(rows *sql.Rows) error,
	args ...interface{},
) error {
	rows, err := stmt.QueryContext(ctx, args...)
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

func (es EventStore) insertTransaction(ctx context.Context, tx *sql.Tx, events []eventstore.SerializedEvent, txId uuid.UUID) error {
	aggregateIds := map[account.ID]bool{}
	for _, event := range events {
		aggregateIds[event.AggregateId] = true
	}

	insertTransactionsStmt := tx.StmtContext(ctx, es.insertTransactionStmt)
	for aggregateId := range aggregateIds {
		if _, err := insertTransactionsStmt.ExecContext(ctx, aggregateId.UUID[:], txId[:]); err != nil {
			return err
		}
	}
	return nil
}

func (es EventStore) insertEvents(ctx context.Context, tx *sql.Tx, events []eventstore.SerializedEvent, txId uuid.UUID) error {
	insertEventsStmt := tx.StmtContext(ctx, es.appendEventStmt)

	for _, event := range events {
		if _, err := insertEventsStmt.ExecContext(ctx, event.AggregateId.UUID[:], event.Seq, txId[:], event.EventType, event.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (es EventStore) updateSnapshots(ctx context.Context, tx *sql.Tx, snapshots []eventstore.SerializedEvent) error {
	deleteSnapshotsStmt := tx.StmtContext(ctx, es.removeSnapshotStmt)
	insertSnapshotsStmt := tx.StmtContext(ctx, es.storeSnapshotStmt)
	for _, snapshot := range snapshots {
		if _, err := deleteSnapshotsStmt.ExecContext(ctx, snapshot.AggregateId.UUID[:]); err != nil {
			return err
		}
		if _, err := insertSnapshotsStmt.ExecContext(ctx, snapshot.AggregateId.UUID[:], snapshot.Seq, snapshot.EventType, snapshot.Payload); err != nil {
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

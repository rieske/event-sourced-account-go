package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"io"
	"log"
)

type EventStore struct {
	db                    *sql.DB
	selectEventsStmt      *sql.Stmt
	selectSnapshotStmt    *sql.Stmt
	selectTransactionStmt *sql.Stmt
}

const (
	appendEventSql  = "INSERT INTO Event(aggregateId, sequenceNumber, transactionId, eventType, payload) VALUES($1, $2, $3, $4, $5)"
	selectEventsSql = "SELECT sequenceNumber, eventType, payload FROM Event WHERE aggregateId = $1 AND sequenceNumber > $2 ORDER BY sequenceNumber ASC"

	storeSnapshotSql = "INSERT INTO Snapshot(aggregateId, sequenceNumber, eventType, payload) VALUES($1, $2, $3, $4) " +
		"ON CONFLICT (aggregateId) DO UPDATE SET sequenceNumber=$2, eventType=$3, payload=$4"
	selectSnapshotSql = "SELECT sequenceNumber, eventType, payload FROM Snapshot WHERE aggregateId = $1"

	insertTransactionSql = "INSERT INTO Transaction(aggregateId, transactionId) VALUES($1, $2)"
	selectTransactionSql = "SELECT aggregateId FROM Transaction WHERE aggregateId = $1 AND transactionId = $2"
)

func MigrateSchema(db *sql.DB, schemaLocation string) {
	driver, err := postgres.WithInstance(db, &postgres.Config{})
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
	}
}

func prepareStatementOrPanic(db *sql.DB, sql string) *sql.Stmt {
	stmt, err := db.Prepare(sql)
	if err != nil {
		panic(err)
	}
	return stmt
}

func (es *EventStore) Events(ctx context.Context, id account.ID, version int) ([]eventstore.SerializedEvent, error) {
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
		id, version,
	)

	return events, err
}

func (es *EventStore) LoadSnapshot(ctx context.Context, id account.ID) (*eventstore.SerializedEvent, error) {
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
		id,
	)

	return snapshot, err
}

func (es *EventStore) TransactionExists(ctx context.Context, id account.ID, txId uuid.UUID) (bool, error) {
	transactionExists := false

	err := sqlSelect(
		ctx,
		es.selectTransactionStmt,
		func(rows *sql.Rows) error {
			transactionExists = rows.Next()
			return nil
		},
		id, txId,
	)

	return transactionExists, err
}

func (es *EventStore) Append(ctx context.Context, events []eventstore.SerializedEvent, snapshots []eventstore.SerializedEvent, txId uuid.UUID) error {
	if err := es.append(ctx, events, snapshots, txId); err != nil {
		return toConcurrentModification(err)
	}
	return nil
}

func (es *EventStore) append(ctx context.Context, events []eventstore.SerializedEvent, snapshots []eventstore.SerializedEvent, txId uuid.UUID) error {
	return es.withTransaction(ctx, func(tx *sql.Tx) error {
		if err := insertEvents(ctx, tx, events, txId); err != nil {
			return err
		}
		if err := insertTransaction(ctx, tx, events, txId); err != nil {
			return err
		}
		if len(snapshots) != 0 {
			if err := updateSnapshots(ctx, tx, snapshots); err != nil {
				return err
			}
		}
		return nil
	})
}

func (es *EventStore) withTransaction(ctx context.Context, doInTx func(tx *sql.Tx) error) error {
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

func insertTransaction(ctx context.Context, tx *sql.Tx, events []eventstore.SerializedEvent, txId uuid.UUID) error {
	/*insertTransactionsStmt, err := tx.PrepareContext(ctx, insertTransactionSql)
	if err != nil {
		return err
	}
	defer closeResource(insertTransactionsStmt)*/

	aggregateIds := map[account.ID]bool{}
	for _, event := range events {
		aggregateIds[event.AggregateId] = true
	}
	for aggregateId := range aggregateIds {
		if _, err := tx.ExecContext(ctx, insertTransactionSql, aggregateId, txId); err != nil {
			return err
		}
	}
	return nil
}

func insertEvents(ctx context.Context, tx *sql.Tx, events []eventstore.SerializedEvent, txId uuid.UUID) error {
	insertEventsStmt, err := tx.PrepareContext(ctx, appendEventSql)
	if err != nil {
		return err
	}
	defer closeResource(insertEventsStmt)

	for _, event := range events {
		if _, err := insertEventsStmt.ExecContext(ctx, event.AggregateId, event.Seq, txId, event.EventType, event.Payload); err != nil {
			return err
		}
	}
	return nil
}

func updateSnapshots(ctx context.Context, tx *sql.Tx, snapshots []eventstore.SerializedEvent) error {
	insertSnapshotsStmt, err := tx.PrepareContext(ctx, storeSnapshotSql)
	if err != nil {
		return err
	}
	defer closeResource(insertSnapshotsStmt)
	for _, snapshot := range snapshots {
		if _, err := insertSnapshotsStmt.ExecContext(ctx, snapshot.AggregateId, snapshot.Seq, snapshot.EventType, snapshot.Payload); err != nil {
			return err
		}
	}
	return nil
}

func toConcurrentModification(err error) error {
	var e *pq.Error
	if errors.As(err, &e) && e.Code == "23505" {
		return account.ConcurrentModification
	}
	return err
}

func closeResource(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Printf("Could not close resource: %v\n", err)
	}
}

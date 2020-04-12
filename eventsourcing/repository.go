package eventsourcing

import (
	"context"

	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
)

type repository struct {
	store             EventStore
	snapshotFrequency int
}

type transaction func(*account.Account) error
type biTransaction func(*account.Account, *account.Account) error

func NewAccountRepository(es EventStore, snapshotFrequency int) *repository {
	return &repository{store: es, snapshotFrequency: snapshotFrequency}
}

func (r repository) newEventStream() *eventStream {
	return newEventStream(r.store, r.snapshotFrequency)
}

func (r repository) query(ctx context.Context, id account.ID) (*account.Snapshot, error) {
	a := r.loadAggregate(ctx, id)
	if a.err != nil {
		return nil, a.err
	}
	snapshot := a.acc.Snapshot()
	return &snapshot, nil
}

func (r repository) create(ctx context.Context, id account.ID, tx transaction) error {
	a, err := r.newAggregate(ctx, id)
	if err != nil {
		return err
	}
	return a.transact(ctx, tx, uuid.New())
}

func (r repository) transact(ctx context.Context, id account.ID, txId uuid.UUID, tx transaction) error {
	a := r.loadAggregate(ctx, id)
	if transactionExists, err := r.store.TransactionExists(ctx, id, txId); err != nil || transactionExists {
		return err
	}
	return a.transact(ctx, tx, txId)
}

func (r repository) biTransact(ctx context.Context, sourceId, targetId account.ID, txId uuid.UUID, tx biTransaction) error {
	es := r.newEventStream()
	source, err := es.replay(ctx, sourceId)
	if err != nil {
		return err
	}
	target, err := es.replay(ctx, targetId)
	if err != nil {
		return err
	}

	if transactionExists, err := r.store.TransactionExists(ctx, sourceId, txId); err != nil || transactionExists {
		return err
	}
	if transactionExists, err := r.store.TransactionExists(ctx, targetId, txId); err != nil || transactionExists {
		return err
	}

	if err := tx(source, target); err != nil {
		return err
	}

	return es.commit(ctx, txId)
}

func (r repository) aggregateExists(ctx context.Context, id account.ID) (bool, error) {
	events, err := r.store.Events(ctx, id, 0)
	return len(events) != 0, err
}

func (r repository) newAggregate(ctx context.Context, id account.ID) (*aggregate, error) {
	a := aggregate{}
	aggregateExists, err := r.aggregateExists(ctx, id)
	if err != nil {
		return nil, err
	}
	if aggregateExists {
		a.err = account.Exists
		return &a, nil
	}
	a.es = r.newEventStream()
	a.acc = account.New(a.es)
	return &a, nil
}

func (r repository) loadAggregate(ctx context.Context, id account.ID) *aggregate {
	a := aggregate{}
	a.es = r.newEventStream()
	a.acc, a.err = a.es.replay(ctx, id)
	return &a
}

type aggregate struct {
	es  *eventStream
	acc *account.Account
	err error
}

func (a *aggregate) transact(ctx context.Context, tx transaction, txId uuid.UUID) error {
	if a.err != nil {
		return a.err
	}
	if err := tx(a.acc); err != nil {
		return err
	}

	return a.es.commit(ctx, txId)
}

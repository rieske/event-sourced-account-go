package eventsourcing

import (
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

func (r repository) query(id account.Id) (*account.Snapshot, error) {
	a := r.loadAggregate(id)
	if a.err != nil {
		return nil, a.err
	}
	snapshot := a.acc.Snapshot()
	return &snapshot, nil
}

func (r repository) create(id account.Id, tx transaction) error {
	a, err := r.newAggregate(id)
	if err != nil {
		return err
	}
	return a.transact(tx, uuid.New())
}

func (r repository) transact(id account.Id, txId uuid.UUID, tx transaction) error {
	a := r.loadAggregate(id)
	transactionExists, err := r.store.TransactionExists(id, txId)
	if err != nil {
		return err
	}
	if transactionExists {
		return nil
	}
	return a.transact(tx, txId)
}

func (r repository) biTransact(sourceId, targetId account.Id, txId uuid.UUID, tx biTransaction) error {
	es := r.newEventStream()
	source, err := es.replay(sourceId)
	if err != nil {
		return err
	}
	target, err := es.replay(targetId)
	if err != nil {
		return err
	}

	transactionExists, err := r.transactionExists(sourceId, targetId, txId)
	if err != nil {
		return err
	}
	if transactionExists {
		return nil
	}
	if err := tx(source, target); err != nil {
		return err
	}

	return es.commit(txId)
}

func (r repository) transactionExists(sourceId, targetId account.Id, txId uuid.UUID) (bool, error) {
	sourceTxExists, err := r.store.TransactionExists(sourceId, txId)
	if err != nil {
		return false, err
	}
	targetTxExists, err := r.store.TransactionExists(targetId, txId)
	if err != nil {
		return false, err
	}
	return sourceTxExists || targetTxExists, nil
}

func (r repository) aggregateExists(id account.Id) (bool, error) {
	events, err := r.store.Events(id, 0)
	if err != nil {
		return false, err
	}
	return len(events) != 0, nil
}

func (r repository) newAggregate(id account.Id) (*aggregate, error) {
	a := aggregate{}
	aggregateExists, err := r.aggregateExists(id)
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

func (r repository) loadAggregate(id account.Id) *aggregate {
	a := aggregate{}
	a.es = r.newEventStream()
	a.acc, a.err = a.es.replay(id)
	return &a
}

type aggregate struct {
	es  *eventStream
	acc *account.Account
	err error
}

func (a *aggregate) transact(tx transaction, txId uuid.UUID) error {
	if a.err != nil {
		return a.err
	}
	err := tx(a.acc)
	if err != nil {
		return err
	}

	return a.es.commit(txId)
}

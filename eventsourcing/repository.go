package eventsourcing

import (
	"errors"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
)

type repository struct {
	store             eventStore
	snapshotFrequency int
}

type transaction func(*account.Account) error
type biTransaction func(*account.Account, *account.Account) error

func NewAccountRepository(es eventStore, snapshotFrequency int) *repository {
	return &repository{es, snapshotFrequency}
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
	a := r.newAggregate(id)
	return a.transact(tx, uuid.New())
}

func (r repository) transact(id account.Id, txId uuid.UUID, tx transaction) error {
	if r.store.TransactionExists(id, txId) {
		return nil
	}

	a := r.loadAggregate(id)
	return a.transact(tx, txId)
}

func (r repository) biTransact(sourceId, targetId account.Id, txId uuid.UUID, tx biTransaction) error {
	if r.store.TransactionExists(sourceId, txId) || r.store.TransactionExists(targetId, txId) {
		return nil
	}

	es := r.newEventStream()
	source, err := es.replay(sourceId)
	if err != nil {
		return err
	}
	target, err := es.replay(targetId)
	if err != nil {
		return err
	}

	err = tx(source, target)
	if err != nil {
		return err
	}

	return es.commit(txId)
}

func (r repository) aggregateExists(id account.Id) bool {
	events := r.store.Events(id, 0)
	return len(events) != 0
}

func (r repository) newAggregate(id account.Id) aggregate {
	a := aggregate{}
	if r.aggregateExists(id) {
		a.err = errors.New("account already exists")
		return a
	}
	a.es = r.newEventStream()
	a.acc = account.NewAccount(a.es)
	return a
}

func (r repository) loadAggregate(id account.Id) aggregate {
	a := aggregate{}
	a.es = r.newEventStream()
	a.acc, a.err = a.es.replay(id)
	return a
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

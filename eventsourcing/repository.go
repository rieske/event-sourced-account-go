package eventsourcing

import (
	"errors"
	"github.com/rieske/event-sourced-account-go/account"
)

type Repository struct {
	store             eventStore
	snapshotFrequency int
}

type transaction func(*account.Account) error
type biTransaction func(*account.Account, *account.Account) error

func NewAccountRepository(es eventStore, snapshotFrequency int) *Repository {
	return &Repository{es, snapshotFrequency}
}

func (r Repository) newEventStream() *transactionalEventStream {
	return NewEventStream(r.store, r.snapshotFrequency)
}

func (r Repository) Query(id account.Id) (*account.Snapshot, error) {
	a := r.loadAggregate(id)
	if a.err != nil {
		return nil, a.err
	}
	snapshot := a.acc.Snapshot()
	return &snapshot, nil
}

// TODO: refactor to Create
func (r Repository) Open(id account.Id, ownerId account.OwnerId) error {
	a := r.newAggregate(id)
	return a.transact(func(a *account.Account) error {
		return a.Open(id, ownerId)
	})
}

func (r Repository) Transact(id account.Id, tx transaction) error {
	a := r.loadAggregate(id)
	return a.transact(tx)
}

func (r Repository) BiTransact(sourceId, targetId account.Id, tx biTransaction) error {
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

	return es.commit()
}

func (r Repository) aggregateExists(id account.Id) bool {
	events := r.store.Events(id, 0)
	return len(events) != 0
}

func (r Repository) newAggregate(id account.Id) aggregate {
	a := aggregate{}
	if r.aggregateExists(id) {
		a.err = errors.New("account already exists")
		return a
	}
	a.es = r.newEventStream()
	acc := account.NewAccount(a.es)
	a.acc = acc
	return a
}

func (r Repository) loadAggregate(id account.Id) aggregate {
	a := aggregate{}
	a.es = r.newEventStream()
	a.acc, a.err = a.es.replay(id)
	return a
}

type aggregate struct {
	es  *transactionalEventStream
	acc *account.Account
	err error
}

func (a *aggregate) transact(tx transaction) error {
	if a.err != nil {
		return a.err
	}
	err := tx(a.acc)
	if err != nil {
		return err
	}

	return a.es.commit()
}

package account

import "errors"

type Repository struct {
	store *eventStore
}

type transaction func(*account) error
type biTransaction func(*account, *account) error

func NewAccountRepository(es eventStore) *Repository {
	return &Repository{&es}
}

func (r *Repository) Query(id AggregateId) (*Snapshot, error) {
	a := r.loadAggregate(id)
	if a.err != nil {
		return nil, a.err
	}
	snapshot := a.acc.Snapshot()
	return &snapshot, nil
}

func (r *Repository) Open(id AggregateId, ownerId OwnerId) error {
	a := r.newAggregate(id)
	return a.transact(func(a *account) error {
		return a.Open(id, ownerId)
	})
}

func (r *Repository) Transact(id AggregateId, tx transaction) error {
	a := r.loadAggregate(id)
	return a.transact(tx)
}

func (r *Repository) BiTransact(sourceId, targetId AggregateId, tx biTransaction) error {
	es := NewEventStream(*r.store)
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

func (r *Repository) aggregateExists(id AggregateId) bool {
	events := (*r.store).Events(id, 0)
	return len(events) != 0
}

type aggregate struct {
	es  *transactionalEventStream
	acc *account
	err error
}

func (r *Repository) newAggregate(id AggregateId) aggregate {
	a := aggregate{}
	if r.aggregateExists(id) {
		a.err = errors.New("account already exists")
		return a
	}
	a.es = NewEventStream(*r.store)
	acc := newAccount(a.es)
	a.acc = acc
	return a
}

func (r *Repository) loadAggregate(id AggregateId) aggregate {
	a := aggregate{}
	a.es = NewEventStream(*r.store)
	a.acc, a.err = a.es.replay(id)
	return a
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

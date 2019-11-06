package account

import "errors"

type Repository struct {
	store *eventStore
}

type transaction func(*account) (Event, error)

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
	return a.transact(func(a *account) (Event, error) {
		return a.Open(id, ownerId)
	})
}

func (r *Repository) Transact(id AggregateId, tx transaction) error {
	a := r.loadAggregate(id)
	return a.transact(tx)
}

func (r *Repository) BiTransact(sourceId, targetId AggregateId, sourceTransaction, targetTransaction transaction) error {
	es := NewEventStream(*r.store)
	source, err := es.replay(sourceId)
	if err != nil {
		return err
	}
	target, err := es.replay(targetId)
	if err != nil {
		return err
	}

	sourceEvent, err := sourceTransaction(source)
	if err != nil {
		return err
	}
	es.append(sourceEvent, *source.id)

	targetEvent, err := targetTransaction(target)
	if err != nil {
		return err
	}
	es.append(targetEvent, *target.id)

	return es.commit()
}

func (r *Repository) aggregateExists(id AggregateId) bool {
	events := (*r.store).Events(id, 0)
	return len(events) != 0
}

type aggregate struct {
	es  *eventStream
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
	a.acc = &account{}
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
	event, err := tx(a.acc)
	if err != nil {
		return err
	}
	a.es.append(event, *a.acc.id)

	return a.es.commit()
}

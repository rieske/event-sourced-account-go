package account

import "errors"

type Repository struct {
	store *eventStore
}

type accountOperation func(*account) (Event, error)

func NewAccountRepository(es eventStore) *Repository {
	return &Repository{&es}
}

func (r *Repository) aggregateExists(id AggregateId) bool {
	events := (*r.store).Events(id, 0)
	return len(events) != 0
}

func (r *Repository) Open(id AggregateId, ownerId OwnerId) error {
	openAccount := func(a *account) (Event, error) {
		return a.Open(id, ownerId)
	}
	a := r.newAggregate(id)
	return a.operate(openAccount)
}

func (r *Repository) Deposit(id AggregateId, amount int64) error {
	deposit := func(a *account) (Event, error) {
		return a.Deposit(amount)
	}

	a := r.loadAggregate(id)
	return a.operate(deposit)
}

type aggregate struct {
	store *eventStore
	es    *eventStream
	acc   *account
	err   error
}

func (r *Repository) newAggregate(id AggregateId) aggregate {
	a := aggregate{
		store: r.store,
	}
	if r.aggregateExists(id) {
		a.err = errors.New("account already exists")
		return a
	}
	a.es = NewEventStream(*a.store)
	a.acc = &account{}
	return a
}

func (r *Repository) loadAggregate(id AggregateId) aggregate {
	a := aggregate{
		store: r.store,
	}
	a.es = NewEventStream(*a.store)
	a.acc, a.err = a.es.replay(id)
	return a
}

func (a *aggregate) operate(operation accountOperation) error {
	if a.err != nil {
		return a.err
	}
	event, err := operation(a.acc)
	if err != nil {
		return err
	}
	a.es.append(event, *a.acc.id)

	return a.es.commit()
}

package account

import "errors"

type Repository struct {
	store *eventStore
}

type accountOperation func(*account) (Event, error)

func NewAccountRepository(es eventStore) *Repository {
	return &Repository{&es}
}

func (r *Repository) Open(id AggregateId, ownerId OwnerId) error {
	a := r.newAggregate(id)
	return a.operate(func(a *account) (Event, error) {
		return a.Open(id, ownerId)
	})
}

func (r *Repository) Deposit(id AggregateId, amount int64) error {
	a := r.loadAggregate(id)
	return a.operate(func(a *account) (Event, error) {
		return a.Deposit(amount)
	})
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

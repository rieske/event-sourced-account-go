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
	if r.aggregateExists(id) {
		return errors.New("account already exists")
	}

	es := NewEventStream(*r.store)
	a := account{}
	event, err := a.Open(id, ownerId)
	if err != nil {
		return err
	}
	es.append(event, id)

	return es.commit()
}

func (r *Repository) Deposit(id AggregateId, amount int64) error {
	deposit := func(a *account) (Event, error) {
		return a.Deposit(amount)
	}
	return r.doWithAccount(id, deposit)
}

func (r *Repository) doWithAccount(id AggregateId, operation accountOperation) error {
	es := NewEventStream(*r.store)

	a, err := es.replay(id)
	if err != nil {
		return err
	}

	event, err := operation(a)
	if err != nil {
		return err
	}
	es.append(event, id)

	return es.commit()
}

package account

import "errors"

type AccountRepository struct {
	store *eventStore
}

func NewAccountRepository(es eventStore) *AccountRepository {
	return &AccountRepository{&es}
}

func (r *AccountRepository) aggregateExists(id AggregateId) bool {
	events := (*r.store).Events(id, 0)
	return len(events) != 0
}

func (r *AccountRepository) Open(id AggregateId, ownerId OwnerId) error {
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

func (r *AccountRepository) Deposit(id AggregateId, amount int64) error {
	es := NewEventStream(*r.store)

	a, err := es.replay(id)
	if err != nil {
		return err
	}

	event, err := a.Deposit(amount)
	if err != nil {
		return err
	}
	es.append(event, id)

	return es.commit()
}

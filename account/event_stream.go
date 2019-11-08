package account

import (
	"errors"
)

type sequencedEvent struct {
	aggregateId AggregateId
	seq         int
	event       Event
}

type eventStore interface {
	Events(id AggregateId, version int) []Event
	Append(events []sequencedEvent) error
}

type eventStream interface {
	append(e Event, a *account, id AggregateId)
}

type transactionalEventStream struct {
	eventStore       *eventStore
	versions         map[AggregateId]int
	uncomittedEvents []sequencedEvent
}

func NewEventStream(es eventStore) *transactionalEventStream {
	return &transactionalEventStream{&es, map[AggregateId]int{}, nil}
}

func (s *transactionalEventStream) replay(id AggregateId) (*account, error) {
	events := (*s.eventStore).Events(id, 0)
	var currentVersion = 0

	a := NewAccount(s)
	for _, e := range events {
		e.apply(&a)
		currentVersion += 1
	}

	if currentVersion == 0 {
		return nil, errors.New("Aggregate not found")
	}

	s.versions[id] = currentVersion
	return &a, nil
}

func (s *transactionalEventStream) append(e Event, a *account, id AggregateId) {
	e.apply(a)
	s.versions[id] = s.versions[id] + 1
	se := sequencedEvent{id, s.versions[id], e}
	s.uncomittedEvents = append(s.uncomittedEvents, se)
}

func (s *transactionalEventStream) commit() error {
	err := (*s.eventStore).Append(s.uncomittedEvents)
	if err != nil {
		return err
	}
	s.uncomittedEvents = nil
	return nil
}

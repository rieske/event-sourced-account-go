package eventsourcing

import (
	"errors"
	"github.com/rieske/event-sourced-account-go/account"
)

type sequencedEvent struct {
	aggregateId account.AggregateId
	seq         int
	event       account.Event
}

type eventStore interface {
	Events(id account.AggregateId, version int) []account.Event
	Append(events []sequencedEvent, snapshots map[account.AggregateId]sequencedEvent) error
	LoadSnapshot(id account.AggregateId) *sequencedEvent
}

type transactionalEventStream struct {
	eventStore           eventStore
	snapshotFrequency    int
	versions             map[account.AggregateId]int
	uncommittedEvents    []sequencedEvent
	uncommittedSnapshots map[account.AggregateId]sequencedEvent
}

func NewEventStream(es eventStore) *transactionalEventStream {
	return &transactionalEventStream{
		eventStore:           es,
		versions:             map[account.AggregateId]int{},
		uncommittedSnapshots: map[account.AggregateId]sequencedEvent{},
	}
}

func NewSnapshottingEventStream(es eventStore, snapshotFrequency int) *transactionalEventStream {
	return &transactionalEventStream{
		eventStore:           es,
		snapshotFrequency:    snapshotFrequency,
		versions:             map[account.AggregateId]int{},
		uncommittedSnapshots: map[account.AggregateId]sequencedEvent{},
	}
}

func (s *transactionalEventStream) applySnapshot(id account.AggregateId, a account.Aggregate) int {
	snapshot := s.eventStore.LoadSnapshot(id)
	if snapshot.event != nil {
		snapshot.event.Apply(a)
		return snapshot.seq
	}
	return 0
}

func (s *transactionalEventStream) replay(id account.AggregateId) (*account.Account, error) {
	a := account.NewAccount(s)
	var currentVersion = s.applySnapshot(id, a)
	events := s.eventStore.Events(id, currentVersion)

	for _, e := range events {
		e.Apply(a)
		currentVersion += 1
	}

	if currentVersion == 0 {
		return nil, errors.New("aggregate not found")
	}

	s.versions[id] = currentVersion
	return a, nil
}

func (s *transactionalEventStream) Append(e account.Event, a account.Aggregate, id account.AggregateId) {
	e.Apply(a)
	version := s.versions[id] + 1
	s.versions[id] = version
	se := sequencedEvent{id, version, e}
	s.uncommittedEvents = append(s.uncommittedEvents, se)
	if s.snapshotFrequency != 0 && version%s.snapshotFrequency == 0 {
		s.uncommittedSnapshots[id] = sequencedEvent{id, version, a.Snapshot()}
	}
}

func (s *transactionalEventStream) commit() error {
	err := s.eventStore.Append(s.uncommittedEvents, s.uncommittedSnapshots)
	if err != nil {
		return err
	}
	s.uncommittedEvents = nil
	s.uncommittedSnapshots = map[account.AggregateId]sequencedEvent{}
	return nil
}

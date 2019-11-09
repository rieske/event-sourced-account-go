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
	Append(events []sequencedEvent, snapshots map[AggregateId]sequencedEvent) error
	LoadSnapshot(id AggregateId) *sequencedEvent
}

type eventStream interface {
	append(e Event, a *account, id AggregateId)
}

type transactionalEventStream struct {
	eventStore           eventStore
	snapshotFrequency    int
	versions             map[AggregateId]int
	uncommittedEvents    []sequencedEvent
	uncommittedSnapshots map[AggregateId]sequencedEvent
}

func NewEventStream(es eventStore) *transactionalEventStream {
	return &transactionalEventStream{
		eventStore:           es,
		versions:             map[AggregateId]int{},
		uncommittedSnapshots: map[AggregateId]sequencedEvent{},
	}
}

func NewSnapshottingEventStream(es eventStore, snapshotFrequency int) *transactionalEventStream {
	return &transactionalEventStream{
		eventStore:           es,
		snapshotFrequency:    snapshotFrequency,
		versions:             map[AggregateId]int{},
		uncommittedSnapshots: map[AggregateId]sequencedEvent{},
	}
}

func (s *transactionalEventStream) applySnapshot(id AggregateId, a *account) int {
	snapshot := s.eventStore.LoadSnapshot(id)
	if snapshot.event != nil {
		snapshot.event.apply(a)
		return snapshot.seq
	}
	return 0
}

func (s *transactionalEventStream) replay(id AggregateId) (*account, error) {
	a := newAccount(s)
	var currentVersion = s.applySnapshot(id, a)
	events := s.eventStore.Events(id, currentVersion)

	for _, e := range events {
		e.apply(a)
		currentVersion += 1
	}

	if currentVersion == 0 {
		return nil, errors.New("Aggregate not found")
	}

	s.versions[id] = currentVersion
	return a, nil
}

func (s *transactionalEventStream) append(e Event, a *account, id AggregateId) {
	e.apply(a)
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
	s.uncommittedSnapshots = map[AggregateId]sequencedEvent{}
	return nil
}

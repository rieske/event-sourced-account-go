package eventsourcing

import (
	"errors"
	"github.com/rieske/event-sourced-account-go/account"
)

type sequencedEvent struct {
	aggregateId account.Id
	seq         int
	event       account.Event
}

type eventStore interface {
	Events(id account.Id, version int) []sequencedEvent
	Append(events []sequencedEvent, snapshots map[account.Id]sequencedEvent) error
	LoadSnapshot(id account.Id) *sequencedEvent
}

type eventStream struct {
	eventStore           eventStore
	snapshotFrequency    int
	versions             map[account.Id]int
	uncommittedEvents    []sequencedEvent
	uncommittedSnapshots map[account.Id]sequencedEvent
}

func newEventStream(es eventStore, snapshotFrequency int) *eventStream {
	if snapshotFrequency < 0 {
		panic("snapshot frequency can not be negative")
	}
	return &eventStream{
		eventStore:           es,
		snapshotFrequency:    snapshotFrequency,
		versions:             map[account.Id]int{},
		uncommittedSnapshots: map[account.Id]sequencedEvent{},
	}
}

func (s *eventStream) applySnapshot(id account.Id) (*account.Account, int) {
	a := account.NewAccount(s)
	snapshot := s.eventStore.LoadSnapshot(id)
	if snapshot.event != nil {
		snapshot.event.Apply(a)
		return a, snapshot.seq
	}
	return a, 0
}

func (s *eventStream) replay(id account.Id) (*account.Account, error) {
	a, currentVersion := s.applySnapshot(id)
	events := s.eventStore.Events(id, currentVersion)

	for _, e := range events {
		e.event.Apply(a)
		currentVersion += 1
	}

	if currentVersion == 0 {
		return nil, errors.New("aggregate not found")
	}

	s.versions[id] = currentVersion
	return a, nil
}

func (s *eventStream) Append(e account.Event, a *account.Account, id account.Id) {
	e.Apply(a)
	version := s.versions[id] + 1
	s.versions[id] = version
	se := sequencedEvent{id, version, e}
	s.uncommittedEvents = append(s.uncommittedEvents, se)
	if s.snapshotFrequency != 0 && version%s.snapshotFrequency == 0 {
		s.uncommittedSnapshots[id] = sequencedEvent{id, version, a.Snapshot()}
	}
}

func (s *eventStream) commit() error {
	err := s.eventStore.Append(s.uncommittedEvents, s.uncommittedSnapshots)
	if err != nil {
		return err
	}
	s.uncommittedEvents = nil
	s.uncommittedSnapshots = map[account.Id]sequencedEvent{}
	return nil
}

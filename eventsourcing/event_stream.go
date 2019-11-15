package eventsourcing

import (
	"errors"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
)

type eventStore interface {
	Events(id account.Id, version int) ([]eventstore.SequencedEvent, error)
	Append(events []eventstore.SequencedEvent, snapshots map[account.Id]eventstore.SequencedEvent, txId uuid.UUID) error
	LoadSnapshot(id account.Id) (*eventstore.SequencedEvent, error)
	TransactionExists(id account.Id, txId uuid.UUID) bool
}

type eventStream struct {
	eventStore           eventStore
	snapshotFrequency    int
	versions             map[account.Id]int
	uncommittedEvents    []eventstore.SequencedEvent
	uncommittedSnapshots map[account.Id]eventstore.SequencedEvent
}

func newEventStream(es eventStore, snapshotFrequency int) *eventStream {
	if snapshotFrequency < 0 {
		panic("snapshot frequency can not be negative")
	}
	return &eventStream{
		eventStore:           es,
		snapshotFrequency:    snapshotFrequency,
		versions:             map[account.Id]int{},
		uncommittedSnapshots: map[account.Id]eventstore.SequencedEvent{},
	}
}

func (s *eventStream) applySnapshot(id account.Id) (*account.Account, int, error) {
	a := account.NewAccount(s)
	snapshot, err := s.eventStore.LoadSnapshot(id)
	if err != nil {
		return nil, 0, err
	}
	if snapshot.Event != nil {
		snapshot.Event.Apply(a)
		return a, snapshot.Seq, nil
	}
	return a, 0, nil
}

func (s *eventStream) replay(id account.Id) (*account.Account, error) {
	a, currentVersion, err := s.applySnapshot(id)
	if err != nil {
		return nil, err
	}
	events, err := s.eventStore.Events(id, currentVersion)
	if err != nil {
		return nil, err
	}

	for _, e := range events {
		e.Event.Apply(a)
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
	se := eventstore.SequencedEvent{id, version, e}
	s.uncommittedEvents = append(s.uncommittedEvents, se)
	if s.snapshotFrequency != 0 && version%s.snapshotFrequency == 0 {
		s.uncommittedSnapshots[id] = eventstore.SequencedEvent{id, version, a.Snapshot()}
	}
}

func (s *eventStream) commit(txId uuid.UUID) error {
	err := s.eventStore.Append(s.uncommittedEvents, s.uncommittedSnapshots, txId)
	if err != nil {
		return err
	}
	s.uncommittedEvents = nil
	s.uncommittedSnapshots = map[account.Id]eventstore.SequencedEvent{}
	return nil
}

package eventsourcing

import (
	"context"
	"log"

	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
)

type EventStore interface {
	Events(ctx context.Context, id account.ID, version int) ([]eventstore.SequencedEvent, error)
	Append(ctx context.Context, events []eventstore.SequencedEvent, snapshots map[account.ID]eventstore.SequencedEvent, txId uuid.UUID) error
	LoadSnapshot(ctx context.Context, id account.ID) (eventstore.SequencedEvent, error)
	TransactionExists(ctx context.Context, id account.ID, txId uuid.UUID) (bool, error)
}

type eventStream struct {
	eventStore           EventStore
	snapshotFrequency    int
	versions             map[account.ID]int
	uncommittedEvents    []eventstore.SequencedEvent
	uncommittedSnapshots map[account.ID]eventstore.SequencedEvent
}

func newEventStream(es EventStore, snapshotFrequency int) *eventStream {
	if snapshotFrequency < 0 {
		log.Panic("snapshot frequency can not be negative")
	}
	return &eventStream{
		eventStore:           es,
		snapshotFrequency:    snapshotFrequency,
		versions:             map[account.ID]int{},
		uncommittedSnapshots: map[account.ID]eventstore.SequencedEvent{},
	}
}

func (s *eventStream) applySnapshot(ctx context.Context, id account.ID) (*account.Account, int, error) {
	a := account.New(s)
	snapshot, err := s.eventStore.LoadSnapshot(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	if snapshot.Event != nil {
		snapshot.Event.Apply(a)
		return a, snapshot.Seq, nil
	}
	return a, 0, nil
}

func (s *eventStream) replay(ctx context.Context, id account.ID) (*account.Account, error) {
	a, currentVersion, err := s.applySnapshot(ctx, id)
	if err != nil {
		return nil, err
	}
	events, err := s.eventStore.Events(ctx, id, currentVersion)
	if err != nil {
		return nil, err
	}

	for _, e := range events {
		e.Event.Apply(a)
	}
	currentVersion += len(events)

	if currentVersion == 0 {
		return nil, account.NotFound
	}

	s.versions[id] = currentVersion
	return a, nil
}

func (s *eventStream) Append(e account.Event, a *account.Account, id account.ID) {
	e.Apply(a)
	version := s.versions[id] + 1
	s.versions[id] = version
	se := eventstore.SequencedEvent{AggregateId: id, Seq: version, Event: e}
	s.uncommittedEvents = append(s.uncommittedEvents, se)
	if s.snapshotFrequency != 0 && version%s.snapshotFrequency == 0 {
		s.uncommittedSnapshots[id] = eventstore.SequencedEvent{AggregateId: id, Seq: version, Event: a.Snapshot()}
	}
}

func (s *eventStream) commit(ctx context.Context, txId uuid.UUID) error {
	if err := s.eventStore.Append(ctx, s.uncommittedEvents, s.uncommittedSnapshots, txId); err != nil {
		return err
	}
	s.uncommittedEvents = nil
	s.uncommittedSnapshots = map[account.ID]eventstore.SequencedEvent{}
	return nil
}

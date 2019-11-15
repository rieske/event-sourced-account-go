package eventstore

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
)

type SerializedEvent struct {
	AggregateId account.Id
	Seq         int
	Payload     []byte
	EventType   int
	txId        uuid.UUID
}

type eventSerializer interface {
	SerializeEvent(e SequencedEvent) (*SerializedEvent, error)
	DeserializeEvent(s *SerializedEvent) (*SequencedEvent, error)
}

type eventStore interface {
	Events(id account.Id, version int) []*SerializedEvent
	Append(events []*SerializedEvent, snapshots map[account.Id]*SerializedEvent) error
	LoadSnapshot(id account.Id) *SerializedEvent
	TransactionExists(id account.Id, txId uuid.UUID) bool
}

type serializingEventStore struct {
	store      eventStore
	serializer eventSerializer
}

func (s serializingEventStore) Events(id account.Id, version int) ([]SequencedEvent, error) {
	serializedEvents := s.store.Events(id, version)
	var events []SequencedEvent
	for _, serializedEvent := range serializedEvents {
		event, err := s.serializer.DeserializeEvent(serializedEvent)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, nil
}

func (s serializingEventStore) Append(events []SequencedEvent, snapshots map[account.Id]SequencedEvent, txId uuid.UUID) error {
	var serializedEvents []*SerializedEvent
	for _, event := range events {
		serializedEvent, err := s.serializer.SerializeEvent(event)
		if err != nil {
			return err
		}
		serializedEvent.txId = txId
		serializedEvents = append(serializedEvents, serializedEvent)
	}
	serializedSnapshots := map[account.Id]*SerializedEvent{}
	for id, snapshot := range snapshots {
		serializedSnapshot, err := s.serializer.SerializeEvent(snapshot)
		if err != nil {
			return err
		}
		serializedSnapshot.txId = txId
		serializedSnapshots[id] = serializedSnapshot
	}
	return s.store.Append(serializedEvents, serializedSnapshots)
}

func (s serializingEventStore) LoadSnapshot(id account.Id) (*SequencedEvent, error) {
	serializedSnapshot := s.store.LoadSnapshot(id)
	snapshot, err := s.serializer.DeserializeEvent(serializedSnapshot)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s serializingEventStore) TransactionExists(id account.Id, txId uuid.UUID) bool {
	return s.store.TransactionExists(id, txId)
}

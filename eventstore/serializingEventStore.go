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
}

type eventSerializer interface {
	SerializeEvent(e SequencedEvent) (SerializedEvent, error)
	DeserializeEvent(s SerializedEvent) (SequencedEvent, error)
}

type eventStore interface {
	Events(id account.Id, version int) ([]SerializedEvent, error)
	Append(events []SerializedEvent, snapshots map[account.Id]SerializedEvent, txId uuid.UUID) error
	LoadSnapshot(id account.Id) (*SerializedEvent, error)
	TransactionExists(id account.Id, txId uuid.UUID) (bool, error)
}

type serializingEventStore struct {
	store      eventStore
	serializer eventSerializer
}

func (s serializingEventStore) Events(id account.Id, version int) ([]SequencedEvent, error) {
	serializedEvents, err := s.store.Events(id, version)
	if err != nil {
		return nil, err
	}
	var events []SequencedEvent
	for _, serializedEvent := range serializedEvents {
		event, err := s.serializer.DeserializeEvent(serializedEvent)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (s serializingEventStore) Append(events []SequencedEvent, snapshots map[account.Id]SequencedEvent, txId uuid.UUID) error {
	var serializedEvents []SerializedEvent
	for _, event := range events {
		serializedEvent, err := s.serializer.SerializeEvent(event)
		if err != nil {
			return err
		}
		serializedEvents = append(serializedEvents, serializedEvent)
	}
	serializedSnapshots := map[account.Id]SerializedEvent{}
	for id, snapshot := range snapshots {
		serializedSnapshot, err := s.serializer.SerializeEvent(snapshot)
		if err != nil {
			return err
		}
		serializedSnapshots[id] = serializedSnapshot
	}
	return s.store.Append(serializedEvents, serializedSnapshots, txId)
}

func (s serializingEventStore) LoadSnapshot(id account.Id) (SequencedEvent, error) {
	serializedSnapshot, err := s.store.LoadSnapshot(id)
	if err != nil {
		return SequencedEvent{}, err
	}
	if serializedSnapshot == nil {
		return SequencedEvent{}, nil
	}
	snapshot, err := s.serializer.DeserializeEvent(*serializedSnapshot)
	if err != nil {
		return SequencedEvent{}, err
	}
	return snapshot, nil
}

func (s serializingEventStore) TransactionExists(id account.Id, txId uuid.UUID) (bool, error) {
	return s.store.TransactionExists(id, txId)
}

package eventstore

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
)

type inmemoryStore struct {
	events       []SequencedEvent
	snapshots    map[account.ID]SequencedEvent
	transactions map[account.ID][]uuid.UUID
	mutex        sync.RWMutex
}

func NewInMemoryStore() *inmemoryStore {
	return &inmemoryStore{
		snapshots:    map[account.ID]SequencedEvent{},
		transactions: map[account.ID][]uuid.UUID{},
	}
}

func (es *inmemoryStore) Events(ctx context.Context, id account.ID, version int) ([]SequencedEvent, error) {
	events := make([]SequencedEvent, 0, len(es.events))
	for _, e := range es.events {
		if e.AggregateId == id && e.Seq > version {
			events = append(events, e)
		}
	}
	return events, nil
}

func (es *inmemoryStore) LoadSnapshot(ctx context.Context, id account.ID) (SequencedEvent, error) {
	es.mutex.RLock()
	defer es.mutex.RUnlock()

	return es.snapshots[id], nil
}

func (es *inmemoryStore) TransactionExists(ctx context.Context, id account.ID, txId uuid.UUID) (bool, error) {
	es.mutex.RLock()
	defer es.mutex.RUnlock()

	return es.transactionExists(es.transactions[id], txId)
}

// the mutex here simulates what a persistence engine of choice should do - ensure consistency
// Events can only be written in sequence per aggregate.
// One way to ensure this in RDB - primary key on (aggregateId, sequenceNumber)
// Event writes have to happen in a transaction - either all get written or none
func (es *inmemoryStore) Append(ctx context.Context, events []SequencedEvent, snapshots map[account.ID]SequencedEvent, txId uuid.UUID) error {
	es.mutex.Lock()
	defer es.mutex.Unlock()

	err := es.validateConsistency(events, txId)
	if err != nil {
		return err
	}

	for _, e := range events {
		es.events = append(es.events, e)
		es.transactions[e.AggregateId] = append(es.transactions[e.AggregateId], txId)
	}
	for id, snapshot := range snapshots {
		es.snapshots[id] = snapshot
	}
	return nil
}

func (es *inmemoryStore) validateConsistency(events []SequencedEvent, txId uuid.UUID) error {
	aggregateVersions := map[account.ID]int{}

	for _, e := range events {
		currentVersion := aggregateVersions[e.AggregateId]
		if currentVersion == 0 {
			currentVersion = es.latestVersion(e.AggregateId)
		}
		transactionExists, err := es.transactionExists(es.transactions[e.AggregateId], txId)
		if err != nil {
			return err
		}
		if transactionExists {
			return account.ConcurrentModification
		}
		if e.Seq <= currentVersion {
			return account.ConcurrentModification
		}
		aggregateVersions[e.AggregateId] = e.Seq
	}
	return nil
}

func (es *inmemoryStore) latestVersion(id account.ID) int {
	latestVersion := 0
	for _, e := range es.events {
		if e.AggregateId == id {
			latestVersion = e.Seq
		}
	}
	return latestVersion
}

func (es *inmemoryStore) transactionExists(transactions []uuid.UUID, txId uuid.UUID) (bool, error) {
	for _, tx := range transactions {
		if tx == txId {
			return true, nil
		}
	}
	return false, nil
}

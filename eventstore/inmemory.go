package eventstore

import (
	"errors"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"sync"
)

type SequencedEvent struct {
	AggregateId account.Id
	Seq         int
	Event       account.Event
}

type inmemoryEeventstore struct {
	events       []SequencedEvent
	snapshots    map[account.Id]SequencedEvent
	transactions map[account.Id][]uuid.UUID
	mutex        sync.RWMutex
}

func NewInMemoryStore() *inmemoryEeventstore {
	return &inmemoryEeventstore{
		snapshots:    map[account.Id]SequencedEvent{},
		transactions: map[account.Id][]uuid.UUID{},
	}
}

func (es *inmemoryEeventstore) Events(id account.Id, version int) []SequencedEvent {
	var events []SequencedEvent
	for _, e := range es.events {
		if e.AggregateId == id && e.Seq > version {
			events = append(events, e)
		}
	}
	return events
}

func (es *inmemoryEeventstore) LoadSnapshot(id account.Id) SequencedEvent {
	es.mutex.RLock()
	defer es.mutex.RUnlock()

	return es.snapshots[id]
}

func (es *inmemoryEeventstore) TransactionExists(id account.Id, txId uuid.UUID) bool {
	es.mutex.RLock()
	defer es.mutex.RUnlock()

	return es.transactionExists(es.transactions[id], txId)
}

// the mutex here simulates what a persistence engine of choice should do - ensure consistency
// Events can only be written in sequence per aggregate.
// One way to ensure this in RDB - primary key on (aggregateId, sequenceNumber)
// Event writes have to happen in a transaction - either all get written or none
func (es *inmemoryEeventstore) Append(events []SequencedEvent, snapshots map[account.Id]SequencedEvent, txId uuid.UUID) error {
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

func (es *inmemoryEeventstore) validateConsistency(events []SequencedEvent, txId uuid.UUID) error {
	aggregateVersions := map[account.Id]int{}

	for _, e := range events {
		currentVersion := aggregateVersions[e.AggregateId]
		if currentVersion == 0 {
			currentVersion = es.latestVersion(e.AggregateId)
		}
		if es.transactionExists(es.transactions[e.AggregateId], txId) {
			return errors.New("concurrent modification error")
		}
		if e.Seq <= currentVersion {
			return errors.New("concurrent modification error")
		}
		aggregateVersions[e.AggregateId] = e.Seq
	}
	return nil
}

func (es *inmemoryEeventstore) latestVersion(id account.Id) int {
	latestVersion := 0
	for _, e := range es.events {
		if e.AggregateId == id {
			latestVersion = e.Seq
		}
	}
	return latestVersion
}

func (es *inmemoryEeventstore) transactionExists(transactions []uuid.UUID, txId uuid.UUID) bool {
	for _, tx := range transactions {
		if tx == txId {
			return true
		}
	}
	return false
}

package eventsourcing

import (
	"errors"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"sync"
)

type inmemoryEeventstore struct {
	events       []sequencedEvent
	snapshots    map[account.Id]sequencedEvent
	transactions map[account.Id][]uuid.UUID
	mutex        sync.RWMutex
}

func newInMemoryStore() *inmemoryEeventstore {
	return &inmemoryEeventstore{
		snapshots:    map[account.Id]sequencedEvent{},
		transactions: map[account.Id][]uuid.UUID{},
	}
}

func (es *inmemoryEeventstore) Events(id account.Id, version int) []sequencedEvent {
	var events []sequencedEvent
	for _, e := range es.events {
		if e.aggregateId == id && e.seq > version {
			events = append(events, e)
		}
	}
	return events
}

func (es *inmemoryEeventstore) LoadSnapshot(id account.Id) sequencedEvent {
	es.mutex.RLock()
	snapshot := es.snapshots[id]
	es.mutex.RUnlock()
	return snapshot
}

func (es *inmemoryEeventstore) TransactionExists(id account.Id, txId uuid.UUID) bool {
	es.mutex.RLock()
	transactions := es.transactions[id]
	es.mutex.RUnlock()
	return es.transactionExists(transactions, txId)
}

// the mutex here simulates what a persistence engine of choice should do - ensure consistency
// Events can only be written in sequence per aggregate.
// One way to ensure this in RDB - primary key on (aggregateId, sequenceNumber)
// Event writes have to happen in a transaction - either all get written or none
func (es *inmemoryEeventstore) Append(events []sequencedEvent, snapshots map[account.Id]sequencedEvent, txId uuid.UUID) error {
	es.mutex.Lock()
	err := es.validateConsistency(events, txId)
	if err != nil {
		es.mutex.Unlock()
		return err
	}

	for _, e := range events {
		es.events = append(es.events, e)
		es.transactions[e.aggregateId] = append(es.transactions[e.aggregateId], txId)
	}
	for id, snapshot := range snapshots {
		es.snapshots[id] = snapshot
	}
	es.mutex.Unlock()
	return nil
}

func (es *inmemoryEeventstore) validateConsistency(events []sequencedEvent, txId uuid.UUID) error {
	aggregateVersions := map[account.Id]int{}

	for _, e := range events {
		currentVersion := aggregateVersions[e.aggregateId]
		if currentVersion == 0 {
			currentVersion = es.latestVersion(e.aggregateId)
		}
		if es.transactionExists(es.transactions[e.aggregateId], txId) {
			return errors.New("concurrent modification error")
		}
		if e.seq <= currentVersion {
			return errors.New("concurrent modification error")
		}
		aggregateVersions[e.aggregateId] = e.seq
	}
	return nil
}

func (es *inmemoryEeventstore) latestVersion(id account.Id) int {
	latestVersion := 0
	for _, e := range es.events {
		if e.aggregateId == id {
			latestVersion = e.seq
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

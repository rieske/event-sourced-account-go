package eventsourcing

import (
	"errors"
	"github.com/rieske/event-sourced-account-go/account"
	"sync"
)

type inmemoryEeventstore struct {
	events    []sequencedEvent
	snapshots map[account.Id]sequencedEvent
	mutex     sync.Mutex
}

func newInMemoryStore() *inmemoryEeventstore {
	return &inmemoryEeventstore{snapshots: map[account.Id]sequencedEvent{}}
}

func (es *inmemoryEeventstore) Events(id account.Id, version int) []sequencedEvent {
	var events []sequencedEvent
	for _, e := range es.events {
		if e.aggregateId == id {
			events = append(events, e)
		}
	}
	return events
}

func (es *inmemoryEeventstore) LoadSnapshot(id account.Id) *sequencedEvent {
	es.mutex.Lock()
	snapshot := es.snapshots[id]
	es.mutex.Unlock()
	return &snapshot
}

// the mutex here simulates what a persistence engine of choice should do - ensure consistency
// Events can only be written in sequence per aggregate.
// One way to ensure this in RDB - primary key on (aggregateId, sequenceNumber)
// Event writes have to happen in a transaction - either all get written or none
func (es *inmemoryEeventstore) Append(events []sequencedEvent, snapshots map[account.Id]sequencedEvent) error {
	es.mutex.Lock()
	for _, e := range events {
		if e.seq <= es.latestVersion(e.aggregateId) {
			es.mutex.Unlock()
			return errors.New("concurrent modification error")
		}
		es.events = append(es.events, e)
	}
	for id, snapshot := range snapshots {
		es.snapshots[id] = snapshot
	}
	es.mutex.Unlock()
	return nil
}

func (es *inmemoryEeventstore) latestVersion(id account.Id) int {
	aggVersions := map[account.Id]int{}
	for _, e := range es.events {
		aggVersions[e.aggregateId] = e.seq
	}
	return aggVersions[id]
}
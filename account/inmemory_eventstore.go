package account

import (
	"errors"
	"sync"
)

type inmemoryEeventstore struct {
	events []sequencedEvent
	mutex  sync.Mutex
}

func (es *inmemoryEeventstore) Events(id AggregateId, version int) []Event {
	events := []Event{}
	for _, e := range es.events {
		if e.aggregateId == id {
			events = append(events, e.event)
		}
	}
	return events
}

// the mutex here simulates what a persistence engine of choice should do - ensure consistency
// Events can only be written in sequence per aggregate.
// One way to ensure this in RDB - primary key on (aggregateId, sequenceNumber)
// Event writes have to happen in a transaction - either all get written or none
func (es *inmemoryEeventstore) Append(events []sequencedEvent) error {
	es.mutex.Lock()
	for _, e := range events {
		if e.seq <= es.latestVersion(e.aggregateId) {
			es.mutex.Unlock()
			return errors.New("Concurrent modification error")
		}

		es.events = append(es.events, e)
	}
	es.mutex.Unlock()
	return nil
}

func (es *inmemoryEeventstore) latestVersion(id AggregateId) int {
	aggVersions := map[AggregateId]int{}
	for _, e := range es.events {
		aggVersions[e.aggregateId] = e.seq
	}
	return aggVersions[id]
}

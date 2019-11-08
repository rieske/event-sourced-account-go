package account

import "testing"

type esTestFixture struct {
	t     *testing.T
	store *inmemoryEeventstore
}

func newInMemoryFixture(t *testing.T) esTestFixture {
	return esTestFixture{t, newInMemoryStore()}
}

func (f *esTestFixture) givenEvents(events []sequencedEvent) {
	err := f.store.Append(events, map[AggregateId]sequencedEvent{})
	expectNoError(f.t, err)
}

func (f *esTestFixture) givenSnapshot(snapshot sequencedEvent) {
	err := f.store.Append(nil, map[AggregateId]sequencedEvent{
		snapshot.aggregateId: snapshot,
	})
	expectNoError(f.t, err)
}

func (f *esTestFixture) makeEventStream() *transactionalEventStream {
	return NewEventStream(f.store)
}

func (f *esTestFixture) makeSnapshottingEventStream(snapshotFrequency int) *transactionalEventStream {
	return NewSnapshottingEventStream(f.store, snapshotFrequency)
}

func (f *esTestFixture) assertPersistedEvent(index int, seq int, aggregateId AggregateId, event Event) {
	seqEvent := f.store.events[index]
	assertEqual(f.t, seqEvent.event, event)
	assertEqual(f.t, seqEvent.aggregateId, aggregateId)
	assertEqual(f.t, seqEvent.seq, seq)
}

func (f *esTestFixture) assertPersistedSnapshot(seq int, aggregateId AggregateId, event Snapshot) {
	snapshot := f.store.LoadSnapshot(aggregateId)
	assertEqual(f.t, snapshot.event, event)
	assertEqual(f.t, snapshot.aggregateId, aggregateId)
	assertEqual(f.t, snapshot.seq, seq)
}

func TestReplayEvents(t *testing.T) {
	id := NewAccountId()
	ownerId := NewOwnerId()
	fixture := newInMemoryFixture(t)
	fixture.givenEvents([]sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
		{id, 2, MoneyDepositedEvent{42, 42}},
	})

	es := fixture.makeEventStream()
	a, err := es.replay(id)
	expectNoError(t, err)
	if a == nil {
		t.Error("Account expected")
	}

	if a.id != id {
		t.Errorf("Account id expected %v, got %v", id, a.id)
	}
	if a.ownerId != ownerId {
		t.Error("owner id should be set")
	}
	if a.open != true {
		t.Error("account should be open")
	}
	expectBalance(t, a, 42)

	version := es.versions[id]
	if version != 2 {
		t.Errorf("Version 2 expected, got: %v", version)
	}
}

func TestReplayEventsWithSnapshot(t *testing.T) {
	id := NewAccountId()
	ownerId := NewOwnerId()
	fixture := newInMemoryFixture(t)
	fixture.givenSnapshot(sequencedEvent{id, 5, Snapshot{id, ownerId, 40, true}})
	fixture.givenEvents([]sequencedEvent{
		{id, 6, MoneyDepositedEvent{10, 50}},
	})

	es := fixture.makeEventStream()
	a, err := es.replay(id)
	expectNoError(t, err)
	if a == nil {
		t.Error("Account expected")
	}

	if a.id != id {
		t.Errorf("Account id expected %v, got %v", id, a.id)
	}
	if a.ownerId != ownerId {
		t.Error("owner id should be set")
	}
	if a.open != true {
		t.Error("account should be open")
	}
	expectBalance(t, a, 50)

	version := es.versions[id]
	if version != 6 {
		t.Errorf("Version 2 expected, got: %v", version)
	}
}

func TestAppendEvent(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := NewAccountId()
	event := AccountOpenedEvent{id, NewOwnerId()}
	a := account{}
	es.append(event, &a, id)

	seqEvent := es.uncommittedEvents[0]
	assertEqual(t, seqEvent.event, event)
	assertEqual(t, seqEvent.aggregateId, id)
	assertEqual(t, seqEvent.seq, 1)
}

func TestCommit(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := NewAccountId()
	event := AccountOpenedEvent{id, NewOwnerId()}
	a := account{}
	es.append(event, &a, id)
	err := es.commit()
	expectNoError(t, err)

	assertEqual(t, 0, len(es.uncommittedEvents))
	assertEqual(t, 0, len(es.uncommittedSnapshots))
	fixture.assertPersistedEvent(0, 1, id, event)
}

func TestAppendEventWithSnapshot(t *testing.T) {
	// given
	fixture := newInMemoryFixture(t)
	id := NewAccountId()
	ownerId := NewOwnerId()
	fixture.givenEvents([]sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
		{id, 2, MoneyDepositedEvent{10, 10}},
		{id, 3, MoneyDepositedEvent{10, 20}},
		{id, 4, MoneyDepositedEvent{10, 30}},
	})

	es := fixture.makeSnapshottingEventStream(5)
	a, err := es.replay(id)
	expectNoError(t, err)

	// when
	event := MoneyDepositedEvent{10, 40}
	es.append(event, a, id)

	// then
	seqEvent := es.uncommittedEvents[0]
	assertEqual(t, seqEvent.event, event)
	assertEqual(t, seqEvent.aggregateId, id)
	assertEqual(t, seqEvent.seq, 5)

	snapshot := es.uncommittedSnapshots[id]
	assertEqual(t, snapshot, sequencedEvent{
		aggregateId: id,
		seq:         5,
		event: Snapshot{
			id:      id,
			ownerId: ownerId,
			balance: 40,
			open:    true,
		},
	})
}

func TestCommitWithSnapshot(t *testing.T) {
	// given
	fixture := newInMemoryFixture(t)
	id := NewAccountId()
	ownerId := NewOwnerId()
	fixture.givenEvents([]sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
		{id, 2, MoneyDepositedEvent{10, 10}},
		{id, 3, MoneyDepositedEvent{10, 20}},
		{id, 4, MoneyDepositedEvent{10, 30}},
	})

	es := fixture.makeSnapshottingEventStream(5)
	a, err := es.replay(id)
	expectNoError(t, err)
	event := MoneyDepositedEvent{10, 40}
	es.append(event, a, id)

	// when
	err = es.commit()
	expectNoError(t, err)

	// then
	assertEqual(t, len(es.uncommittedEvents), 0)
	assertEqual(t, len(es.uncommittedSnapshots), 0)
	fixture.assertPersistedSnapshot(5, id, Snapshot{
		id:      id,
		ownerId: ownerId,
		balance: 40,
		open:    true,
	})
}

func TestCommitInSequence(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := NewAccountId()

	a := account{}
	accountOpenedEvent := AccountOpenedEvent{id, NewOwnerId()}
	es.append(accountOpenedEvent, &a, id)

	depositEvent := MoneyDepositedEvent{42, 42}
	es.append(depositEvent, &a, id)

	err := es.commit()
	expectNoError(t, err)

	assertEqual(t, len(es.uncommittedEvents), 0)
	assertEqual(t, len(fixture.store.events), 2)

	fixture.assertPersistedEvent(0, 1, id, accountOpenedEvent)
	fixture.assertPersistedEvent(1, 2, id, depositEvent)
}

func TestCommitOutOfSequence(t *testing.T) {
	// given account exists
	store := newInMemoryStore()
	es := NewEventStream(store)

	id := NewAccountId()

	a := account{}
	accountOpenedEvent := AccountOpenedEvent{id, NewOwnerId()}
	es.append(accountOpenedEvent, &a, id)
	err := es.commit()
	expectNoError(t, err)

	es1 := NewEventStream(store)
	a1, err := es1.replay(id)
	expectNoError(t, err)

	e1 := MoneyDepositedEvent{10, 10}
	es1.append(e1, a1, id)

	es2 := NewEventStream(store)
	a2, err := es2.replay(id)
	expectNoError(t, err)

	e2 := MoneyDepositedEvent{10, 10}
	es2.append(e2, a2, id)

	err = es1.commit()
	expectNoError(t, err)

	err = es2.commit()
	if err == nil {
		t.Error("Expected concurrent modification error")
	}
}

func assertEqual(t *testing.T, a, b interface{}) {
	if a != b {
		t.Errorf("Expected %v, got %v", b, a)
	}
}

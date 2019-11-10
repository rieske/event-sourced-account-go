package eventsourcing

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/test"
	"testing"
)

type esTestFixture struct {
	t     *testing.T
	store eventStore
}

func newInMemoryFixture(t *testing.T) esTestFixture {
	return esTestFixture{t, newInMemoryStore()}
}

func (f *esTestFixture) givenEvents(events []sequencedEvent) {
	err := f.store.Append(events, map[account.AggregateId]sequencedEvent{})
	test.ExpectNoError(f.t, err)
}

func (f *esTestFixture) givenSnapshot(snapshot sequencedEvent) {
	err := f.store.Append(nil, map[account.AggregateId]sequencedEvent{
		snapshot.aggregateId: snapshot,
	})
	test.ExpectNoError(f.t, err)
}

func (f *esTestFixture) makeEventStream() *transactionalEventStream {
	return NewEventStream(f.store, 0)
}

func (f *esTestFixture) makeSnapshottingEventStream(snapshotFrequency int) *transactionalEventStream {
	return NewEventStream(f.store, snapshotFrequency)
}

func (f *esTestFixture) assertPersistedEvent(index int, seq int, aggregateId account.AggregateId, event account.Event) {
	aggregateEvents := f.store.Events(aggregateId, 0)
	seqEvent := aggregateEvents[index]
	assertEqual(f.t, seqEvent.event, event)
	assertEqual(f.t, seqEvent.aggregateId, aggregateId)
	assertEqual(f.t, seqEvent.seq, seq)
}

func (f *esTestFixture) assertPersistedSnapshot(seq int, aggregateId account.AggregateId, event account.Snapshot) {
	snapshot := f.store.LoadSnapshot(aggregateId)
	assertEqual(f.t, snapshot.event, event)
	assertEqual(f.t, snapshot.aggregateId, aggregateId)
	assertEqual(f.t, snapshot.seq, seq)
}

func TestReplayEvents(t *testing.T) {
	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	fixture := newInMemoryFixture(t)
	fixture.givenEvents([]sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{42, 42}},
	})

	es := fixture.makeEventStream()
	a, err := es.replay(id)
	test.ExpectNoError(t, err)
	if a == nil {
		t.Error("Account expected")
	}

	snapshot := a.Snapshot()
	assertEqual(t, snapshot, account.Snapshot{id, ownerId, 42, true})

	version := es.versions[id]
	if version != 2 {
		t.Errorf("Version 2 expected, got: %v", version)
	}
}

func TestReplayEventsWithSnapshot(t *testing.T) {
	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	fixture := newInMemoryFixture(t)
	fixture.givenSnapshot(sequencedEvent{id, 5, account.Snapshot{id, ownerId, 40, true}})
	fixture.givenEvents([]sequencedEvent{
		{id, 6, account.MoneyDepositedEvent{10, 50}},
	})

	es := fixture.makeEventStream()
	a, err := es.replay(id)
	test.ExpectNoError(t, err)
	if a == nil {
		t.Error("Account expected")
	}

	snapshot := a.Snapshot()
	assertEqual(t, snapshot, account.Snapshot{id, ownerId, 50, true})

	version := es.versions[id]
	if version != 6 {
		t.Errorf("Version 2 expected, got: %v", version)
	}
}

func TestAppendEvent(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := account.NewAccountId()
	event := account.AccountOpenedEvent{id, account.NewOwnerId()}
	a := account.Account{}
	es.Append(event, &a, id)

	seqEvent := es.uncommittedEvents[0]
	assertEqual(t, seqEvent.event, event)
	assertEqual(t, seqEvent.aggregateId, id)
	assertEqual(t, seqEvent.seq, 1)
}

func TestCommit(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := account.NewAccountId()
	event := account.AccountOpenedEvent{id, account.NewOwnerId()}
	a := account.Account{}
	es.Append(event, &a, id)
	err := es.commit()
	test.ExpectNoError(t, err)

	assertEqual(t, 0, len(es.uncommittedEvents))
	assertEqual(t, 0, len(es.uncommittedSnapshots))
	fixture.assertPersistedEvent(0, 1, id, event)
}

func TestAppendEventWithSnapshot(t *testing.T) {
	// given
	fixture := newInMemoryFixture(t)
	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	fixture.givenEvents([]sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyDepositedEvent{10, 20}},
		{id, 4, account.MoneyDepositedEvent{10, 30}},
	})

	es := fixture.makeSnapshottingEventStream(5)
	a, err := es.replay(id)
	test.ExpectNoError(t, err)

	// when
	event := account.MoneyDepositedEvent{10, 40}
	es.Append(event, a, id)

	// then
	seqEvent := es.uncommittedEvents[0]
	assertEqual(t, seqEvent.event, event)
	assertEqual(t, seqEvent.aggregateId, id)
	assertEqual(t, seqEvent.seq, 5)

	snapshot := es.uncommittedSnapshots[id]
	assertEqual(t, snapshot, sequencedEvent{
		aggregateId: id,
		seq:         5,
		event:       account.Snapshot{id, ownerId, 40, true},
	})
}

func TestCommitWithSnapshot(t *testing.T) {
	// given
	fixture := newInMemoryFixture(t)
	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	fixture.givenEvents([]sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyDepositedEvent{10, 20}},
		{id, 4, account.MoneyDepositedEvent{10, 30}},
	})

	es := fixture.makeSnapshottingEventStream(5)
	a, err := es.replay(id)
	test.ExpectNoError(t, err)
	event := account.MoneyDepositedEvent{10, 40}
	es.Append(event, a, id)

	// when
	err = es.commit()
	test.ExpectNoError(t, err)

	// then
	assertEqual(t, len(es.uncommittedEvents), 0)
	assertEqual(t, len(es.uncommittedSnapshots), 0)
	fixture.assertPersistedSnapshot(5, id, account.Snapshot{id, ownerId, 40, true})
}

func TestCommitInSequence(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := account.NewAccountId()

	a := account.Account{}
	accountOpenedEvent := account.AccountOpenedEvent{id, account.NewOwnerId()}
	es.Append(accountOpenedEvent, &a, id)

	depositEvent := account.MoneyDepositedEvent{42, 42}
	es.Append(depositEvent, &a, id)

	err := es.commit()
	test.ExpectNoError(t, err)

	assertEqual(t, len(es.uncommittedEvents), 0)
	assertEqual(t, len(fixture.store.Events(id, 0)), 2)

	fixture.assertPersistedEvent(0, 1, id, accountOpenedEvent)
	fixture.assertPersistedEvent(1, 2, id, depositEvent)
}

func TestCommitOutOfSequence(t *testing.T) {
	// given account exists
	store := newInMemoryStore()
	es := NewEventStream(store, 0)

	id := account.NewAccountId()

	a := account.Account{}
	accountOpenedEvent := account.AccountOpenedEvent{id, account.NewOwnerId()}
	es.Append(accountOpenedEvent, &a, id)
	err := es.commit()
	test.ExpectNoError(t, err)

	es1 := NewEventStream(store, 0)
	a1, err := es1.replay(id)
	test.ExpectNoError(t, err)

	e1 := account.MoneyDepositedEvent{10, 10}
	es1.Append(e1, a1, id)

	es2 := NewEventStream(store, 0)
	a2, err := es2.replay(id)
	test.ExpectNoError(t, err)

	e2 := account.MoneyDepositedEvent{10, 10}
	es2.Append(e2, a2, id)

	err = es1.commit()
	test.ExpectNoError(t, err)

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

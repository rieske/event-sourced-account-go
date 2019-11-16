package eventsourcing

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/stretchr/testify/assert"
	"testing"
)

type esTestFixture struct {
	t     *testing.T
	store EventStore
}

func newInMemoryFixture(t *testing.T) *esTestFixture {
	return &esTestFixture{t, eventstore.NewInMemoryStore()}
}

func (f *esTestFixture) givenEvents(events []eventstore.SequencedEvent) {
	err := f.store.Append(events, map[account.Id]eventstore.SequencedEvent{}, uuid.New())
	assert.NoError(f.t, err)
}

func (f *esTestFixture) givenSnapshot(snapshot eventstore.SequencedEvent) {
	err := f.store.Append(
		nil,
		map[account.Id]eventstore.SequencedEvent{
			snapshot.AggregateId: snapshot,
		},
		uuid.New(),
	)
	assert.NoError(f.t, err)
}

func (f *esTestFixture) makeEventStream() *eventStream {
	return newEventStream(f.store, 0)
}

func (f *esTestFixture) makeSnapshottingEventStream(snapshotFrequency int) *eventStream {
	return newEventStream(f.store, snapshotFrequency)
}

func (f *esTestFixture) assertPersistedEvent(index int, seq int, aggregateId account.Id, event account.Event) {
	aggregateEvents, err := f.store.Events(aggregateId, 0)
	assert.NoError(f.t, err)
	seqEvent := aggregateEvents[index]

	assert.Equal(f.t, event, seqEvent.Event)
	assert.Equal(f.t, aggregateId, seqEvent.AggregateId)
	assert.Equal(f.t, seq, seqEvent.Seq)
}

func (f *esTestFixture) assertPersistedSnapshot(seq int, aggregateId account.Id, event account.Snapshot) {
	snapshot, err := f.store.LoadSnapshot(aggregateId)

	assert.NoError(f.t, err)
	assert.Equal(f.t, event, snapshot.Event)
	assert.Equal(f.t, aggregateId, snapshot.AggregateId)
	assert.Equal(f.t, seq, snapshot.Seq)
}

func TestReplayEvents(t *testing.T) {
	fixture := newInMemoryFixture(t)
	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	fixture.givenEvents([]eventstore.SequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{42, 42}},
	})

	es := fixture.makeEventStream()
	a, err := es.replay(id)
	assert.NoError(t, err)
	if a == nil {
		t.Error("Account expected")
	}

	snapshot := a.Snapshot()
	assert.Equal(t, account.Snapshot{id, ownerId, 42, true}, snapshot)

	version := es.versions[id]
	if version != 2 {
		t.Errorf("Version 2 expected, got: %v", version)
	}
}

func TestReplayEventsWithSnapshot(t *testing.T) {
	fixture := newInMemoryFixture(t)
	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	fixture.givenSnapshot(eventstore.SequencedEvent{id, 5, account.Snapshot{id, ownerId, 40, true}})
	fixture.givenEvents([]eventstore.SequencedEvent{
		{id, 6, account.MoneyDepositedEvent{10, 50}},
	})

	es := fixture.makeEventStream()
	a, err := es.replay(id)
	assert.NoError(t, err)
	if a == nil {
		t.Error("Account expected")
	}

	snapshot := a.Snapshot()
	assert.Equal(t, account.Snapshot{id, ownerId, 50, true}, snapshot)

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
	assert.Equal(t, event, seqEvent.Event)
	assert.Equal(t, id, seqEvent.AggregateId)
	assert.Equal(t, 1, seqEvent.Seq)
}

func TestCommit(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := account.NewAccountId()
	event := account.AccountOpenedEvent{id, account.NewOwnerId()}
	a := account.Account{}
	es.Append(event, &a, id)
	err := es.commit(uuid.New())
	assert.NoError(t, err)

	assert.Equal(t, 0, len(es.uncommittedEvents))
	assert.Equal(t, 0, len(es.uncommittedSnapshots))
	fixture.assertPersistedEvent(0, 1, id, event)
}

func TestAppendEventWithSnapshot(t *testing.T) {
	// given
	fixture := newInMemoryFixture(t)
	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	fixture.givenEvents([]eventstore.SequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyDepositedEvent{10, 20}},
		{id, 4, account.MoneyDepositedEvent{10, 30}},
	})

	es := fixture.makeSnapshottingEventStream(5)
	a, err := es.replay(id)
	assert.NoError(t, err)

	// when
	event := account.MoneyDepositedEvent{10, 40}
	es.Append(event, a, id)

	// then
	seqEvent := es.uncommittedEvents[0]
	assert.Equal(t, event, seqEvent.Event)
	assert.Equal(t, id, seqEvent.AggregateId)
	assert.Equal(t, 5, seqEvent.Seq)

	snapshot := es.uncommittedSnapshots[id]
	assert.Equal(t, eventstore.SequencedEvent{
		AggregateId: id,
		Seq:         5,
		Event:       account.Snapshot{id, ownerId, 40, true},
	}, snapshot)
}

func TestCommitWithSnapshot(t *testing.T) {
	// given
	fixture := newInMemoryFixture(t)
	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	fixture.givenEvents([]eventstore.SequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyDepositedEvent{10, 20}},
		{id, 4, account.MoneyDepositedEvent{10, 30}},
	})

	es := fixture.makeSnapshottingEventStream(5)
	a, err := es.replay(id)
	assert.NoError(t, err)
	event := account.MoneyDepositedEvent{10, 40}
	es.Append(event, a, id)

	// when
	err = es.commit(uuid.New())
	assert.NoError(t, err)

	// then
	assert.Equal(t, 0, len(es.uncommittedEvents))
	assert.Equal(t, 0, len(es.uncommittedSnapshots))
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

	err := es.commit(uuid.New())
	assert.NoError(t, err)

	assert.Equal(t, 0, len(es.uncommittedEvents))
	events, err := fixture.store.Events(id, 0)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(events))

	fixture.assertPersistedEvent(0, 1, id, accountOpenedEvent)
	fixture.assertPersistedEvent(1, 2, id, depositEvent)
}

func TestCommitOutOfSequence(t *testing.T) {
	// given account exists
	store := eventstore.NewInMemoryStore()
	es := newEventStream(store, 0)

	a := account.Account{}
	id := account.NewAccountId()
	accountOpenedEvent := account.AccountOpenedEvent{id, account.NewOwnerId()}
	es.Append(accountOpenedEvent, &a, id)
	err := es.commit(uuid.New())
	assert.NoError(t, err)

	es1 := newEventStream(store, 0)
	a1, err := es1.replay(id)
	assert.NoError(t, err)

	e1 := account.MoneyDepositedEvent{10, 10}
	es1.Append(e1, a1, id)

	es2 := newEventStream(store, 0)
	a2, err := es2.replay(id)
	assert.NoError(t, err)

	e2 := account.MoneyDepositedEvent{10, 10}
	es2.Append(e2, a2, id)

	err = es1.commit(uuid.New())
	assert.NoError(t, err)

	err = es2.commit(uuid.New())
	if err == nil {
		t.Error("Expected concurrent modification error")
	}
}

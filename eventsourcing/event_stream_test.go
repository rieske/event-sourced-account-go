package eventsourcing

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
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
	err := f.store.Append(events, map[account.Id]sequencedEvent{})
	assert.NoError(f.t, err)
}

func (f *esTestFixture) givenSnapshot(snapshot sequencedEvent) {
	err := f.store.Append(nil, map[account.Id]sequencedEvent{
		snapshot.aggregateId: snapshot,
	})
	assert.NoError(f.t, err)
}

func (f *esTestFixture) makeEventStream() *eventStream {
	return newEventStream(f.store, 0)
}

func (f *esTestFixture) makeSnapshottingEventStream(snapshotFrequency int) *eventStream {
	return newEventStream(f.store, snapshotFrequency)
}

func (f *esTestFixture) assertPersistedEvent(index int, seq int, aggregateId account.Id, event account.Event) {
	aggregateEvents := f.store.Events(aggregateId, 0)
	seqEvent := aggregateEvents[index]

	assert.Equal(f.t, event, seqEvent.event)
	assert.Equal(f.t, aggregateId, seqEvent.aggregateId)
	assert.Equal(f.t, seq, seqEvent.seq)
}

func (f *esTestFixture) assertPersistedSnapshot(seq int, aggregateId account.Id, event account.Snapshot) {
	snapshot := f.store.LoadSnapshot(aggregateId)

	assert.Equal(f.t, event, snapshot.event)
	assert.Equal(f.t, aggregateId, snapshot.aggregateId)
	assert.Equal(f.t, seq, snapshot.seq)
}

func TestReplayEvents(t *testing.T) {
	fixture := newInMemoryFixture(t)
	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	fixture.givenEvents([]sequencedEvent{
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
	fixture.givenSnapshot(sequencedEvent{id, 5, account.Snapshot{id, ownerId, 40, true}})
	fixture.givenEvents([]sequencedEvent{
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
	assert.Equal(t, event, seqEvent.event)
	assert.Equal(t, id, seqEvent.aggregateId)
	assert.Equal(t, 1, seqEvent.seq)
}

func TestCommit(t *testing.T) {
	fixture := newInMemoryFixture(t)
	es := fixture.makeEventStream()

	id := account.NewAccountId()
	event := account.AccountOpenedEvent{id, account.NewOwnerId()}
	a := account.Account{}
	es.Append(event, &a, id)
	err := es.commit()
	assert.NoError(t, err)

	assert.Equal(t, 0, len(es.uncommittedEvents))
	assert.Equal(t, 0, len(es.uncommittedSnapshots))
	fixture.assertPersistedEvent(0, 1, id, event)
}

func TestAppendEventWithSnapshot(t *testing.T) {
	// given
	fixture := newInMemoryFixture(t)
	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	fixture.givenEvents([]sequencedEvent{
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
	assert.Equal(t, event, seqEvent.event)
	assert.Equal(t, id, seqEvent.aggregateId)
	assert.Equal(t, 5, seqEvent.seq)

	snapshot := es.uncommittedSnapshots[id]
	assert.Equal(t, sequencedEvent{
		aggregateId: id,
		seq:         5,
		event:       account.Snapshot{id, ownerId, 40, true},
	}, snapshot)
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
	assert.NoError(t, err)
	event := account.MoneyDepositedEvent{10, 40}
	es.Append(event, a, id)

	// when
	err = es.commit()
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

	err := es.commit()
	assert.NoError(t, err)

	assert.Equal(t, 0, len(es.uncommittedEvents))
	assert.Equal(t, 2, len(fixture.store.Events(id, 0)))

	fixture.assertPersistedEvent(0, 1, id, accountOpenedEvent)
	fixture.assertPersistedEvent(1, 2, id, depositEvent)
}

func TestCommitOutOfSequence(t *testing.T) {
	// given account exists
	store := newInMemoryStore()
	es := newEventStream(store, 0)

	a := account.Account{}
	id := account.NewAccountId()
	accountOpenedEvent := account.AccountOpenedEvent{id, account.NewOwnerId()}
	es.Append(accountOpenedEvent, &a, id)
	err := es.commit()
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

	err = es1.commit()
	assert.NoError(t, err)

	err = es2.commit()
	if err == nil {
		t.Error("Expected concurrent modification error")
	}
}

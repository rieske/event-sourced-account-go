package serialization

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/stretchr/testify/assert"
	"testing"
)

var serializer = jsonEventSerializer{}

type jsonTestFixture struct {
	event           eventstore.SequencedEvent
	serializedEvent eventstore.SerializedEvent
}

func newSnapshotJsonTestFixture(t *testing.T) jsonTestFixture {
	accountId, err := uuid.Parse("ce7d9c87-e348-406b-933b-0c6dfc0f014e")
	assert.NoError(t, err)
	ownerId, err := uuid.Parse("c2b0bbce-679a-4af5-9a75-8958da9eb02c")
	assert.NoError(t, err)

	return jsonTestFixture{
		event: eventstore.SequencedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Event: account.Snapshot{
				Id:      account.Id{accountId},
				OwnerId: account.OwnerId{ownerId},
				Balance: 20,
				Open:    true,
			},
		},
		serializedEvent: eventstore.SerializedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Payload:     []byte(`{"Id":"ce7d9c87-e348-406b-933b-0c6dfc0f014e","OwnerId":"c2b0bbce-679a-4af5-9a75-8958da9eb02c","Balance":20,"Open":true}`),
			EventType:   snapshot,
		},
	}
}

func newAccountOpenedJsonTestFixture(t *testing.T) jsonTestFixture {
	accountId, err := uuid.Parse("ce7d9c87-e348-406b-933b-0c6dfc0f014e")
	assert.NoError(t, err)
	ownerId, err := uuid.Parse("c2b0bbce-679a-4af5-9a75-8958da9eb02c")
	assert.NoError(t, err)

	return jsonTestFixture{
		event: eventstore.SequencedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Event: account.AccountOpenedEvent{
				AccountId: account.Id{accountId},
				OwnerId:   account.OwnerId{ownerId},
			},
		},
		serializedEvent: eventstore.SerializedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Payload:     []byte(`{"AccountId":"ce7d9c87-e348-406b-933b-0c6dfc0f014e","OwnerId":"c2b0bbce-679a-4af5-9a75-8958da9eb02c"}`),
			EventType:   accountOpened,
		},
	}
}

func newMoneyDepositedJsonTestFixture(t *testing.T) jsonTestFixture {
	accountId, err := uuid.Parse("ce7d9c87-e348-406b-933b-0c6dfc0f014e")
	assert.NoError(t, err)

	return jsonTestFixture{
		event: eventstore.SequencedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Event: account.MoneyDepositedEvent{
				AmountDeposited: 5,
				Balance:         10,
			},
		},
		serializedEvent: eventstore.SerializedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Payload:     []byte(`{"AmountDeposited":5,"Balance":10}`),
			EventType:   moneyDeposited,
		},
	}
}

func newMoneyWithdrawnJsonTestFixture(t *testing.T) jsonTestFixture {
	accountId, err := uuid.Parse("ce7d9c87-e348-406b-933b-0c6dfc0f014e")
	assert.NoError(t, err)

	return jsonTestFixture{
		event: eventstore.SequencedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Event: account.MoneyWithdrawnEvent{
				AmountWithdrawn: 5,
				Balance:         10,
			},
		},
		serializedEvent: eventstore.SerializedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Payload:     []byte(`{"AmountWithdrawn":5,"Balance":10}`),
			EventType:   moneyWithdrawn,
		},
	}
}

func newAccountClosedJsonTestFixture(t *testing.T) jsonTestFixture {
	accountId, err := uuid.Parse("ce7d9c87-e348-406b-933b-0c6dfc0f014e")
	assert.NoError(t, err)

	return jsonTestFixture{
		event: eventstore.SequencedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Event:       account.AccountClosedEvent{},
		},
		serializedEvent: eventstore.SerializedEvent{
			AggregateId: account.Id{accountId},
			Seq:         42,
			Payload:     []byte(`{}`),
			EventType:   accountClosed,
		},
	}
}

func TestJsonSerializeSnapshot(t *testing.T) {
	fixture := newSnapshotJsonTestFixture(t)

	serializedEvent, err := serializer.SerializeEvent(fixture.event)

	assert.NoError(t, err)
	assert.Equal(t, fixture.serializedEvent, serializedEvent)
}

func TestJsonDeserializeSnapshot(t *testing.T) {
	fixture := newSnapshotJsonTestFixture(t)

	event, err := serializer.DeserializeEvent(fixture.serializedEvent)

	assert.NoError(t, err)
	assert.Equal(t, fixture.event, event)
}

func TestJsonSerializeAccountOpened(t *testing.T) {
	fixture := newAccountOpenedJsonTestFixture(t)

	serializedEvent, err := serializer.SerializeEvent(fixture.event)

	assert.NoError(t, err)
	assert.Equal(t, fixture.serializedEvent, serializedEvent)
}

func TestJsonDeserializeAccountOpened(t *testing.T) {
	fixture := newAccountOpenedJsonTestFixture(t)

	event, err := serializer.DeserializeEvent(fixture.serializedEvent)

	assert.NoError(t, err)
	assert.Equal(t, fixture.event, event)
}

func TestJsonSerializeMoneyDeposited(t *testing.T) {
	fixture := newMoneyDepositedJsonTestFixture(t)

	serializedEvent, err := serializer.SerializeEvent(fixture.event)

	assert.NoError(t, err)
	assert.Equal(t, fixture.serializedEvent, serializedEvent)
}

func TestJsonDeserializeMoneyDeposited(t *testing.T) {
	fixture := newMoneyDepositedJsonTestFixture(t)

	event, err := serializer.DeserializeEvent(fixture.serializedEvent)

	assert.NoError(t, err)
	assert.Equal(t, fixture.event, event)
}

func TestJsonSerializeMoneyWithdrawn(t *testing.T) {
	fixture := newMoneyWithdrawnJsonTestFixture(t)

	serializedEvent, err := serializer.SerializeEvent(fixture.event)

	assert.NoError(t, err)
	assert.Equal(t, fixture.serializedEvent, serializedEvent)
}

func TestJsonDeserializeMoneyWithdrawn(t *testing.T) {
	fixture := newMoneyWithdrawnJsonTestFixture(t)

	event, err := serializer.DeserializeEvent(fixture.serializedEvent)

	assert.NoError(t, err)
	assert.Equal(t, fixture.event, event)
}

func TestJsonSerializeAccountClosed(t *testing.T) {
	fixture := newAccountClosedJsonTestFixture(t)

	serializedEvent, err := serializer.SerializeEvent(fixture.event)

	assert.NoError(t, err)
	assert.Equal(t, fixture.serializedEvent, serializedEvent)
}

func TestJsonDeserializeAccountClosed(t *testing.T) {
	fixture := newAccountClosedJsonTestFixture(t)

	event, err := serializer.DeserializeEvent(fixture.serializedEvent)

	assert.NoError(t, err)
	assert.Equal(t, fixture.event, event)
}

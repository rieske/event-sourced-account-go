package serialization_test

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/serialization"
	"github.com/stretchr/testify/assert"
	"testing"
)

var msgpackSerializer = serialization.NewMsgpackEventSerializer()

func TestMsgpackSnapshot(t *testing.T) {
	accountID := account.NewID()
	event := eventstore.SequencedEvent{
		AggregateId: accountID,
		Seq:         42,
		Event: account.Snapshot{
			ID:      accountID,
			OwnerID: account.NewOwnerID(),
			Balance: 20,
			Open:    true,
		},
	}

	serializedEvent, err := msgpackSerializer.SerializeEvent(event)
	assert.NoError(t, err)

	deserializedEvent, err := msgpackSerializer.DeserializeEvent(serializedEvent)
	assert.Equal(t, event, deserializedEvent)
}

func TestMsgpackAccountOpened(t *testing.T) {
	accountID := account.NewID()
	event := eventstore.SequencedEvent{
		AggregateId: accountID,
		Seq:         42,
		Event: account.AccountOpenedEvent{
			AccountID: accountID,
			OwnerID:   account.NewOwnerID(),
		},
	}

	serializedEvent, err := msgpackSerializer.SerializeEvent(event)
	assert.NoError(t, err)

	deserializedEvent, err := msgpackSerializer.DeserializeEvent(serializedEvent)
	assert.Equal(t, event, deserializedEvent)
}

func TestMsgpackMoneyDeposited(t *testing.T) {
	accountID := account.NewID()
	event := eventstore.SequencedEvent{
		AggregateId: accountID,
		Seq:         42,
		Event: account.MoneyDepositedEvent{
			AmountDeposited: 5,
			Balance:         10,
		},
	}

	serializedEvent, err := msgpackSerializer.SerializeEvent(event)
	assert.NoError(t, err)

	deserializedEvent, err := msgpackSerializer.DeserializeEvent(serializedEvent)
	assert.Equal(t, event, deserializedEvent)
}

func TestMsgpackMoneyWithdrawn(t *testing.T) {
	accountID := account.NewID()
	event := eventstore.SequencedEvent{
		AggregateId: accountID,
		Seq:         42,
		Event: account.MoneyWithdrawnEvent{
			AmountWithdrawn: 5,
			Balance:         10,
		},
	}

	serializedEvent, err := msgpackSerializer.SerializeEvent(event)
	assert.NoError(t, err)

	deserializedEvent, err := msgpackSerializer.DeserializeEvent(serializedEvent)
	assert.Equal(t, event, deserializedEvent)
}

func TestMsgpackAccountClosed(t *testing.T) {
	accountID := account.NewID()
	event := eventstore.SequencedEvent{
		AggregateId: accountID,
		Seq:         42,
		Event:       account.AccountClosedEvent{},
	}

	serializedEvent, err := msgpackSerializer.SerializeEvent(event)
	assert.NoError(t, err)

	deserializedEvent, err := msgpackSerializer.DeserializeEvent(serializedEvent)
	assert.Equal(t, event, deserializedEvent)
}

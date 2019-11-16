package serialization

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
)

type jsonEventSerializer struct {
}

func NewJsonEventSerializer() *jsonEventSerializer {
	return &jsonEventSerializer{}
}

const (
	snapshot = iota + 1
	accountOpened
	moneyDeposited
	moneyWithdrawn
	accountClosed
)

func eventTypeAlias(event account.Event) (int, error) {
	switch t := event.(type) {
	case account.Snapshot:
		return snapshot, nil
	case account.AccountOpenedEvent:
		return accountOpened, nil
	case account.MoneyDepositedEvent:
		return moneyDeposited, nil
	case account.MoneyWithdrawnEvent:
		return moneyWithdrawn, nil
	case account.AccountClosedEvent:
		return accountClosed, nil
	default:
		return 0, errors.New(fmt.Sprintf("don't know how to alias %T", t))
	}
}

func deserializeEvent(payload []byte, typeAlias int) (account.Event, error) {
	switch typeAlias {
	case snapshot:
		var event account.Snapshot
		err := json.Unmarshal(payload, &event)
		return event, err
	case accountOpened:
		var event account.AccountOpenedEvent
		err := json.Unmarshal(payload, &event)
		return event, err
	case moneyDeposited:
		var event account.MoneyDepositedEvent
		err := json.Unmarshal(payload, &event)
		return event, err
	case moneyWithdrawn:
		var event account.MoneyWithdrawnEvent
		err := json.Unmarshal(payload, &event)
		return event, err
	case accountClosed:
		var event account.AccountClosedEvent
		err := json.Unmarshal(payload, &event)
		return event, err
	default:
		return nil, errors.New(fmt.Sprintf("Don't know how to deserialize event with type alias %v", typeAlias))
	}
}

func (s jsonEventSerializer) SerializeEvent(e eventstore.SequencedEvent) (eventstore.SerializedEvent, error) {
	serializedPayload, err := json.Marshal(e.Event)
	if err != nil {
		return eventstore.SerializedEvent{}, err
	}
	eventType, err := eventTypeAlias(e.Event)
	if err != nil {
		return eventstore.SerializedEvent{}, err
	}

	serializedEvent := eventstore.SerializedEvent{
		AggregateId: e.AggregateId,
		Seq:         e.Seq,
		Payload:     serializedPayload,
		EventType:   eventType,
	}

	return serializedEvent, nil
}

func (s jsonEventSerializer) DeserializeEvent(se eventstore.SerializedEvent) (eventstore.SequencedEvent, error) {
	event, err := deserializeEvent(se.Payload, se.EventType)
	if err != nil {
		return eventstore.SequencedEvent{}, err
	}
	return eventstore.SequencedEvent{
		AggregateId: se.AggregateId,
		Seq:         se.Seq,
		Event:       event,
	}, nil
}

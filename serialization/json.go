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
	Snapshot = iota + 1
	AccountOpened
	MoneyDeposited
	MoneyWithdrawn
	AccountClosed
)

func eventTypeAlias(event account.Event) (alias int, err error) {
	switch t := event.(type) {
	case account.Snapshot:
		alias = Snapshot
	case account.AccountOpenedEvent:
		alias = AccountOpened
	case account.MoneyDepositedEvent:
		alias = MoneyDeposited
	case account.MoneyWithdrawnEvent:
		alias = MoneyWithdrawn
	case account.AccountClosedEvent:
		alias = AccountClosed
	default:
		err = errors.New(fmt.Sprintf("don't know how to alias %T", t))
	}
	return
}

func (s jsonEventSerializer) SerializeEvent(e eventstore.SequencedEvent) (event eventstore.SerializedEvent, err error) {
	event.AggregateId = e.AggregateId
	event.Seq = e.Seq

	event.Payload, err = json.Marshal(e.Event)
	if err != nil {
		return
	}
	event.EventType, err = eventTypeAlias(e.Event)
	return
}

func (s jsonEventSerializer) DeserializeEvent(se eventstore.SerializedEvent) (event eventstore.SequencedEvent, err error) {
	event.AggregateId = se.AggregateId
	event.Seq = se.Seq
	event.Event, err = deserializeJsonEvent(se.Payload, se.EventType)
	return
}

func deserializeJsonEvent(payload []byte, typeAlias int) (event account.Event, err error) {
	switch typeAlias {
	case Snapshot:
		var e account.Snapshot
		err = json.Unmarshal(payload, &e)
		event = e
	case AccountOpened:
		var e account.AccountOpenedEvent
		err = json.Unmarshal(payload, &e)
		event = e
	case MoneyDeposited:
		var e account.MoneyDepositedEvent
		err = json.Unmarshal(payload, &e)
		event = e
	case MoneyWithdrawn:
		var e account.MoneyWithdrawnEvent
		err = json.Unmarshal(payload, &e)
		event = e
	case AccountClosed:
		var e account.AccountClosedEvent
		err = json.Unmarshal(payload, &e)
		event = e
	default:
		err = errors.New(fmt.Sprintf("Don't know how to deserialize event with type alias %v", typeAlias))
	}
	return
}

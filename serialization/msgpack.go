package serialization

import (
	"errors"
	"fmt"

	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/vmihailenco/msgpack/v4"
)

type msgpackEventSerializer struct {
}

func NewMsgpackEventSerializer() *msgpackEventSerializer {
	return &msgpackEventSerializer{}
}

func (s msgpackEventSerializer) SerializeEvent(e eventstore.SequencedEvent) (event eventstore.SerializedEvent, err error) {
	event.AggregateId = e.AggregateId
	event.Seq = e.Seq

	event.Payload, err = msgpack.Marshal(e.Event)
	if err != nil {
		return
	}
	event.EventType, err = eventTypeAlias(e.Event)
	return
}

func (s msgpackEventSerializer) DeserializeEvent(se eventstore.SerializedEvent) (event eventstore.SequencedEvent, err error) {
	event.AggregateId = se.AggregateId
	event.Seq = se.Seq
	event.Event, err = deserializeMsgpackEvent(se.Payload, se.EventType)
	return
}

func deserializeMsgpackEvent(payload []byte, typeAlias int) (event account.Event, err error) {
	switch typeAlias {
	case Snapshot:
		var e account.Snapshot
		err = msgpack.Unmarshal(payload, &e)
		event = e
	case AccountOpened:
		var e account.AccountOpenedEvent
		err = msgpack.Unmarshal(payload, &e)
		event = e
	case MoneyDeposited:
		var e account.MoneyDepositedEvent
		err = msgpack.Unmarshal(payload, &e)
		event = e
	case MoneyWithdrawn:
		var e account.MoneyWithdrawnEvent
		err = msgpack.Unmarshal(payload, &e)
		event = e
	case AccountClosed:
		var e account.AccountClosedEvent
		err = msgpack.Unmarshal(payload, &e)
		event = e
	default:
		err = errors.New(fmt.Sprintf("Don't know how to deserialize event with type alias %v", typeAlias))
	}
	return
}

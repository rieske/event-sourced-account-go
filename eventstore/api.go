package eventstore

import "github.com/rieske/event-sourced-account-go/account"

type SequencedEvent struct {
	AggregateId account.Id
	Seq         int
	Event       account.Event
}

package eventsourcing_test

import (
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/test"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestEventSourcingInMemory(t *testing.T) {
	store := eventstore.NewInMemoryStore()
	testSuite := test.NewEventsourcingTestSuite(store, 0)
	suite.Run(t, testSuite)
}

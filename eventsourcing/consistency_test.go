package eventsourcing_test

import (
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/test"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestConsistencyInMemory(t *testing.T) {
	store := eventstore.NewInMemoryStore()
	testSuite := test.NewConsistencyTestSuite(100, 8, 0, store)
	suite.Run(t, testSuite)
}

func TestConsistencyInMemoryWithSnapshotting(t *testing.T) {
	store := eventstore.NewInMemoryStore()
	testSuite := test.NewConsistencyTestSuite(100, 8, 5, store)
	suite.Run(t, testSuite)
}

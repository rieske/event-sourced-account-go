// +build integration

package mysql_test

import (
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/serialization"
	"github.com/rieske/event-sourced-account-go/test"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestMysqlIntegration(t *testing.T) {
	eventStore := eventstore.NewSerializingEventStore(store, serialization.NewJsonEventSerializer())

	t.Run("EventsourcingTestSuite", func(t *testing.T) {
		suite.Run(t, test.NewEventsourcingTestSuite(eventStore, 0))
	})

	t.Run("ConsistencyTestSuite", func(t *testing.T) {
		suite.Run(t, test.NewConsistencyTestSuite(10, 8, 0, eventStore))
	})

	t.Run("ConsistencyTestSuiteWithSnapshotting", func(t *testing.T) {
		suite.Run(t, test.NewConsistencyTestSuite(10, 8, 5, eventStore))
	})
}

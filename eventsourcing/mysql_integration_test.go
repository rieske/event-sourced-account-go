// +build integration

package eventsourcing_test

import (
	"database/sql"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/eventstore/mysql"
	"github.com/rieske/event-sourced-account-go/serialization"
	"github.com/rieske/event-sourced-account-go/test"
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestMysqlIntegration(t *testing.T) {
	test.WithMysqlDatabase(func(db *sql.DB) {
		mysql.MigrateSchema(db, "../infrastructure/schema/mysql")
		sqlStore := mysql.NewEventStore(db)
		store := eventstore.NewSerializingEventStore(sqlStore, serialization.NewJsonEventSerializer())

		t.Run("EventsourcingTestSuite", func(t *testing.T) {
			suite.Run(t, &EventsourcingTestSuite{
				Suite:   suite.Suite{},
				service: eventsourcing.NewAccountService(store, 0),
				store:   store,
			})
		})

		t.Run("ConsistencyTestSuite", func(t *testing.T) {
			suite.Run(t, &ConsistencyTestSuite{
				Suite:           suite.Suite{},
				accountService:  eventsourcing.NewAccountService(store, 0),
				operationCount:  10,
				concurrentUsers: 8,
			})
		})

		t.Run("ConsistencyTestSuiteWithSnapshotting", func(t *testing.T) {
			suite.Run(t, &ConsistencyTestSuite{
				Suite:           suite.Suite{},
				accountService:  eventsourcing.NewAccountService(store, 5),
				operationCount:  10,
				concurrentUsers: 8,
			})
		})
	})
}

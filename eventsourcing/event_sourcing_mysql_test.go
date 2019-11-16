package eventsourcing_test

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

func TestEventSourcingSuiteMysql(t *testing.T) {
	suite.Run(t, &EventsourcingTestSuite{suite.Suite{}, nil, nil})
}

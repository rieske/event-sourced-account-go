package eventsourcing_test

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/stretchr/testify/suite"
	"testing"
)

type EventsourcingTestSuite struct {
	suite.Suite
	service *eventsourcing.AccountService
	store   eventsourcing.EventStore
}

func TestEventSourcingInMemory(t *testing.T) {
	store := eventstore.NewInMemoryStore()
	testSuite := EventsourcingTestSuite{
		Suite:   suite.Suite{},
		service: eventsourcing.NewAccountService(store, 0),
		store:   store,
	}

	suite.Run(t, &testSuite)
}

/*func TestEventSourcingInMemoryDb(t *testing.T) {
	suite.Run(t, &EventsourcingTestSuite{suite.Suite{}, nil, nil})
}*/

func (suite *EventsourcingTestSuite) expectEvents(id account.ID, expected []eventstore.SequencedEvent) {
	actual, err := suite.service.Events(id)
	suite.NoError(err)
	suite.Equal(len(expected), len(actual), "Event counts do not match")
	suite.Equal(expected, actual, "events do not match")
}

func (suite *EventsourcingTestSuite) TestOpenAccount() {
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.service.OpenAccount(id, ownerID)
	suite.NoError(err)
}

func (suite *EventsourcingTestSuite) TestCanNotOpenDuplicateAccount() {
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.service.OpenAccount(id, ownerID)
	suite.NoError(err)

	err = suite.service.OpenAccount(id, ownerID)
	suite.EqualError(err, "account already exists")
}

func (suite *EventsourcingTestSuite) TestCanOpenDistinctAccounts() {
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.service.OpenAccount(id, ownerID)
	suite.NoError(err)

	id = account.NewID()
	err = suite.service.OpenAccount(id, ownerID)
	suite.NoError(err)
}

func (suite *EventsourcingTestSuite) TestCanNotDepositWhenNoAccountExists() {
	id := account.NewID()
	err := suite.service.Deposit(id, uuid.New(), 42)

	suite.EqualError(err, "account not found")
}

func (suite *EventsourcingTestSuite) TestEventSourcing_Deposit() {
	// given
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerID}},
		},
		map[account.ID]eventstore.SequencedEvent{},
		uuid.New(),
	)

	// when
	err = suite.service.Deposit(id, uuid.New(), 42)

	// then
	suite.NoError(err)
	suite.expectEvents(id, []eventstore.SequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerID}},
		{id, 2, account.MoneyDepositedEvent{42, 42}},
	})
}

func (suite *EventsourcingTestSuite) TestEventSourcing_Withdraw() {
	// given
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerID}},
			{id, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.ID]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	// when
	err = suite.service.Withdraw(id, uuid.New(), 2)

	// then
	suite.NoError(err)
	suite.expectEvents(id, []eventstore.SequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerID}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyWithdrawnEvent{2, 8}},
	})
}

func (suite *EventsourcingTestSuite) TestTransferMoney() {
	// given
	sourceAccountId, sourceownerID := account.NewID(), account.NewOwnerID()
	targetAccountId, targetownerID := account.NewID(), account.NewOwnerID()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceownerID}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetownerID}},
		},
		map[account.ID]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	// when
	err = suite.service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 2)

	// then
	suite.NoError(err)
	suite.expectEvents(sourceAccountId, []eventstore.SequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceownerID}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		{sourceAccountId, 3, account.MoneyWithdrawnEvent{2, 8}},
	})
	suite.expectEvents(targetAccountId, []eventstore.SequencedEvent{
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetownerID}},
		{targetAccountId, 2, account.MoneyDepositedEvent{2, 2}},
	})
}

func (suite *EventsourcingTestSuite) TestTransferMoneyFailsWithInsufficientBalance() {
	// given
	sourceAccountId, sourceownerID := account.NewID(), account.NewOwnerID()
	targetAccountId, targetownerID := account.NewID(), account.NewOwnerID()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceownerID}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetownerID}},
		},
		map[account.ID]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	// when
	err = suite.service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 11)

	// then
	suite.EqualError(err, "insufficient balance")
	suite.expectEvents(sourceAccountId, []eventstore.SequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceownerID}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
	suite.expectEvents(targetAccountId, []eventstore.SequencedEvent{
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetownerID}},
	})
}

func (suite *EventsourcingTestSuite) TestTransferMoneyFailsWithNonexistentTargetAccount() {
	// given
	sourceAccountId, sourceownerID := account.NewID(), account.NewOwnerID()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceownerID}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.ID]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	targetAccountId := account.NewID()

	// when
	err = suite.service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 3)

	// then
	suite.EqualError(err, "account not found")
	suite.expectEvents(sourceAccountId, []eventstore.SequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceownerID}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
	suite.expectEvents(targetAccountId, nil)
}

func (suite *EventsourcingTestSuite) TestDepositIdempotency() {
	// given
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.service.OpenAccount(id, ownerID)
	suite.NoError(err)

	// when
	transactionId := uuid.New()
	err = suite.service.Deposit(id, transactionId, 10)
	suite.NoError(err)
	err = suite.service.Deposit(id, transactionId, 10)
	suite.NoError(err)

	// then
	snapshot, err := suite.service.QueryAccount(id)
	suite.NoError(err)
	suite.Equal(int64(10), snapshot.Balance)
}

func (suite *EventsourcingTestSuite) TestWithdrawalIdempotency() {
	// given
	id, ownerID := account.NewID(), account.NewOwnerID()
	err := suite.service.OpenAccount(id, ownerID)
	suite.NoError(err)

	err = suite.service.Deposit(id, uuid.New(), 100)
	suite.NoError(err)

	// when
	transactionId := uuid.New()
	err = suite.service.Withdraw(id, transactionId, 10)
	suite.NoError(err)
	err = suite.service.Withdraw(id, transactionId, 10)
	suite.NoError(err)

	// then
	snapshot, err := suite.service.QueryAccount(id)
	suite.NoError(err)
	suite.Equal(int64(90), snapshot.Balance)
}

func (suite *EventsourcingTestSuite) TestTransferIdempotency() {
	// given
	sourceAccountId, sourceownerID := account.NewID(), account.NewOwnerID()
	err := suite.service.OpenAccount(sourceAccountId, sourceownerID)
	suite.NoError(err)
	err = suite.service.Deposit(sourceAccountId, uuid.New(), 100)
	suite.NoError(err)

	targetAccountId, targetownerID := account.NewID(), account.NewOwnerID()
	err = suite.service.OpenAccount(targetAccountId, targetownerID)
	suite.NoError(err)

	// when
	transactionId := uuid.New()
	err = suite.service.Transfer(sourceAccountId, targetAccountId, transactionId, 60)
	suite.NoError(err)
	err = suite.service.Transfer(sourceAccountId, targetAccountId, transactionId, 60)
	suite.NoError(err)

	// then
	snapshot, err := suite.service.QueryAccount(sourceAccountId)
	suite.NoError(err)
	suite.Equal(int64(40), snapshot.Balance)
	snapshot, err = suite.service.QueryAccount(targetAccountId)
	suite.NoError(err)
	suite.Equal(int64(60), snapshot.Balance)
}

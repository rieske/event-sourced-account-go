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

func (suite *EventsourcingTestSuite) expectEvents(id account.Id, expected []eventstore.SequencedEvent) {
	actual, err := suite.service.Events(id)
	suite.NoError(err)
	suite.Equal(len(expected), len(actual), "Event counts do not match")
	suite.Equal(expected, actual, "events do not match")
}

func (suite *EventsourcingTestSuite) TestOpenAccount() {
	id, ownerId := account.NewId(), account.NewOwnerId()
	err := suite.service.OpenAccount(id, ownerId)
	suite.NoError(err)
}

func (suite *EventsourcingTestSuite) TestCanNotOpenDuplicateAccount() {
	id, ownerId := account.NewId(), account.NewOwnerId()
	err := suite.service.OpenAccount(id, ownerId)
	suite.NoError(err)

	err = suite.service.OpenAccount(id, ownerId)
	suite.EqualError(err, "account already exists")
}

func (suite *EventsourcingTestSuite) TestCanOpenDistinctAccounts() {
	id, ownerId := account.NewId(), account.NewOwnerId()
	err := suite.service.OpenAccount(id, ownerId)
	suite.NoError(err)

	id = account.NewId()
	err = suite.service.OpenAccount(id, ownerId)
	suite.NoError(err)
}

func (suite *EventsourcingTestSuite) TestCanNotDepositWhenNoAccountExists() {
	id := account.NewId()
	err := suite.service.Deposit(id, uuid.New(), 42)

	suite.EqualError(err, "aggregate not found")
}

func (suite *EventsourcingTestSuite) TestEventSourcing_Deposit() {
	// given
	id, ownerId := account.NewId(), account.NewOwnerId()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
		},
		map[account.Id]eventstore.SequencedEvent{},
		uuid.New(),
	)

	// when
	err = suite.service.Deposit(id, uuid.New(), 42)

	// then
	suite.NoError(err)
	suite.expectEvents(id, []eventstore.SequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{42, 42}},
	})
}

func (suite *EventsourcingTestSuite) TestEventSourcing_Withdraw() {
	// given
	id, ownerId := account.NewId(), account.NewOwnerId()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
			{id, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	// when
	err = suite.service.Withdraw(id, uuid.New(), 2)

	// then
	suite.NoError(err)
	suite.expectEvents(id, []eventstore.SequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyWithdrawnEvent{2, 8}},
	})
}

func (suite *EventsourcingTestSuite) TestTransferMoney() {
	// given
	sourceAccountId, sourceOwnerId := account.NewId(), account.NewOwnerId()
	targetAccountId, targetOwnerId := account.NewId(), account.NewOwnerId()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.Id]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	// when
	err = suite.service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 2)

	// then
	suite.NoError(err)
	suite.expectEvents(sourceAccountId, []eventstore.SequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		{sourceAccountId, 3, account.MoneyWithdrawnEvent{2, 8}},
	})
	suite.expectEvents(targetAccountId, []eventstore.SequencedEvent{
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		{targetAccountId, 2, account.MoneyDepositedEvent{2, 2}},
	})
}

func (suite *EventsourcingTestSuite) TestTransferMoneyFailsWithInsufficientBalance() {
	// given
	sourceAccountId, sourceOwnerId := account.NewId(), account.NewOwnerId()
	targetAccountId, targetOwnerId := account.NewId(), account.NewOwnerId()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.Id]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	// when
	err = suite.service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 11)

	// then
	suite.EqualError(err, "insufficient balance")
	suite.expectEvents(sourceAccountId, []eventstore.SequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
	suite.expectEvents(targetAccountId, []eventstore.SequencedEvent{
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
	})
}

func (suite *EventsourcingTestSuite) TestTransferMoneyFailsWithNonexistentTargetAccount() {
	// given
	sourceAccountId, sourceOwnerId := account.NewId(), account.NewOwnerId()
	err := suite.store.Append(
		[]eventstore.SequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]eventstore.SequencedEvent{},
		uuid.New(),
	)
	suite.NoError(err)

	targetAccountId := account.NewId()

	// when
	err = suite.service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 3)

	// then
	suite.EqualError(err, "aggregate not found")
	suite.expectEvents(sourceAccountId, []eventstore.SequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
	suite.expectEvents(targetAccountId, nil)
}

func (suite *EventsourcingTestSuite) TestDepositIdempotency() {
	// given
	id, ownerId := account.NewId(), account.NewOwnerId()
	err := suite.service.OpenAccount(id, ownerId)
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
	id, ownerId := account.NewId(), account.NewOwnerId()
	err := suite.service.OpenAccount(id, ownerId)
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
	sourceAccountId, sourceOwnerId := account.NewId(), account.NewOwnerId()
	err := suite.service.OpenAccount(sourceAccountId, sourceOwnerId)
	suite.NoError(err)
	err = suite.service.Deposit(sourceAccountId, uuid.New(), 100)
	suite.NoError(err)

	targetAccountId, targetOwnerId := account.NewId(), account.NewOwnerId()
	err = suite.service.OpenAccount(targetAccountId, targetOwnerId)
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

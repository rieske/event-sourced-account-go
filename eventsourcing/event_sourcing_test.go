package eventsourcing

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
	"testing"
)

func newAccountService() accountService {
	store := newInMemoryStore()
	repo := NewAccountRepository(store, 0)
	return accountService{*repo}
}

func expectEvents(t *testing.T, actual, expected []sequencedEvent) {
	assert.Equal(t, len(expected), len(actual), "event counts do not match")
	assert.Equal(t, expected, actual, "events do not match")
}

func TestOpenAccount(t *testing.T) {
	service := newAccountService()

	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := service.OpenAccount(id, ownerId)
	assert.NoError(t, err)
}

func TestCanNotOpenDuplicateAccount(t *testing.T) {
	service := newAccountService()

	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := service.OpenAccount(id, ownerId)
	assert.NoError(t, err)

	err = service.OpenAccount(id, ownerId)
	assert.EqualError(t, err, "account already exists")
}

func TestCanOpenDistinctAccounts(t *testing.T) {
	service := newAccountService()

	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := service.OpenAccount(id, ownerId)
	assert.NoError(t, err)

	id = account.NewAccountId()
	err = service.OpenAccount(id, ownerId)
	assert.NoError(t, err)
}

func TestCanNotDepositWhenNoAccountExists(t *testing.T) {
	// given
	service := newAccountService()

	// when
	id := account.NewAccountId()
	err := service.Deposit(id, uuid.New(), 42)

	// then
	assert.EqualError(t, err, "aggregate not found")
}

func TestEventSourcing_Deposit(t *testing.T) {
	// given
	service := newAccountService()

	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := service.repo.store.Append(
		[]sequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
		},
		map[account.Id]sequencedEvent{},
		uuid.New(),
	)

	// when
	err = service.Deposit(id, uuid.New(), 42)

	// then
	assert.NoError(t, err)
	expectEvents(t, service.Events(id), []sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{42, 42}},
	})
}

func TestEventSourcing_Withdraw(t *testing.T) {
	// given
	service := newAccountService()

	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := service.repo.store.Append(
		[]sequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
			{id, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]sequencedEvent{},
		uuid.New(),
	)
	assert.NoError(t, err)

	// when
	err = service.Withdraw(id, uuid.New(), 2)

	// then
	assert.NoError(t, err)
	expectEvents(t, service.Events(id), []sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyWithdrawnEvent{2, 8}},
	})
}

func TestTransferMoney(t *testing.T) {
	// given
	service := newAccountService()

	sourceAccountId, sourceOwnerId := account.NewAccountId(), account.NewOwnerId()
	targetAccountId, targetOwnerId := account.NewAccountId(), account.NewOwnerId()
	err := service.repo.store.Append(
		[]sequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.Id]sequencedEvent{},
		uuid.New(),
	)
	assert.NoError(t, err)

	// when
	err = service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 2)

	// then
	assert.NoError(t, err)
	expectEvents(t, service.Events(sourceAccountId), []sequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		{sourceAccountId, 3, account.MoneyWithdrawnEvent{2, 8}},
	})
	expectEvents(t, service.Events(targetAccountId), []sequencedEvent{
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		{targetAccountId, 2, account.MoneyDepositedEvent{2, 2}},
	})
}

func TestTransferMoneyFailsWithInsufficientBalance(t *testing.T) {
	// given
	service := newAccountService()
	sourceAccountId, sourceOwnerId := account.NewAccountId(), account.NewOwnerId()
	targetAccountId, targetOwnerId := account.NewAccountId(), account.NewOwnerId()
	err := service.repo.store.Append(
		[]sequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.Id]sequencedEvent{},
		uuid.New(),
	)
	assert.NoError(t, err)

	// when
	err = service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 11)

	// then
	assert.EqualError(t, err, "insufficient balance")
	expectEvents(t, service.Events(sourceAccountId), []sequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
	expectEvents(t, service.Events(targetAccountId), []sequencedEvent{
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
	})
}

func TestTransferMoneyFailsWithNonexistentTargetAccount(t *testing.T) {
	// given
	service := newAccountService()
	sourceAccountId, sourceOwnerId := account.NewAccountId(), account.NewOwnerId()
	err := service.repo.store.Append(
		[]sequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]sequencedEvent{},
		uuid.New(),
	)
	assert.NoError(t, err)

	targetAccountId := account.NewAccountId()

	// when
	err = service.Transfer(sourceAccountId, targetAccountId, uuid.New(), 3)

	// then
	assert.EqualError(t, err, "aggregate not found")
	expectEvents(t, service.Events(sourceAccountId), []sequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
	expectEvents(t, service.repo.store.Events(targetAccountId, 0), nil)
}

func TestDepositIdempotency(t *testing.T) {
	// given
	service := newAccountService()
	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := service.OpenAccount(id, ownerId)
	assert.NoError(t, err)

	// when
	transactionId := uuid.New()
	err = service.Deposit(id, transactionId, 10)
	assert.NoError(t, err)
	err = service.Deposit(id, transactionId, 10)
	assert.NoError(t, err)

	// then
	snapshot, err := service.QueryAccount(id)
	assert.NoError(t, err)
	assert.Equal(t, int64(10), snapshot.Balance)
}

func TestWithdrawalIdempotency(t *testing.T) {
	// given
	service := newAccountService()
	id, ownerId := account.NewAccountId(), account.NewOwnerId()
	err := service.OpenAccount(id, ownerId)
	assert.NoError(t, err)

	err = service.Deposit(id, uuid.New(), 100)
	assert.NoError(t, err)

	// when
	transactionId := uuid.New()
	err = service.Withdraw(id, transactionId, 10)
	assert.NoError(t, err)
	err = service.Withdraw(id, transactionId, 10)
	assert.NoError(t, err)

	// then
	snapshot, err := service.QueryAccount(id)
	assert.NoError(t, err)
	assert.Equal(t, int64(90), snapshot.Balance)
}

func TestTransferIdempotency(t *testing.T) {
	// given
	service := newAccountService()

	sourceAccountId, sourceOwnerId := account.NewAccountId(), account.NewOwnerId()
	err := service.OpenAccount(sourceAccountId, sourceOwnerId)
	assert.NoError(t, err)
	err = service.Deposit(sourceAccountId, uuid.New(), 100)
	assert.NoError(t, err)

	targetAccountId, targetOwnerId := account.NewAccountId(), account.NewOwnerId()
	err = service.OpenAccount(targetAccountId, targetOwnerId)
	assert.NoError(t, err)

	// when
	transactionId := uuid.New()
	err = service.Transfer(sourceAccountId, targetAccountId, transactionId, 60)
	assert.NoError(t, err)
	err = service.Transfer(sourceAccountId, targetAccountId, transactionId, 60)
	assert.NoError(t, err)

	// then
	snapshot, err := service.QueryAccount(sourceAccountId)
	assert.NoError(t, err)
	assert.Equal(t, int64(40), snapshot.Balance)
	snapshot, err = service.QueryAccount(targetAccountId)
	assert.NoError(t, err)
	assert.Equal(t, int64(60), snapshot.Balance)
}

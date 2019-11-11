package eventsourcing

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAccountRepository_Open(t *testing.T) {
	store := newInMemoryStore()
	repo := NewAccountRepository(store, 0)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := repo.Open(id, ownerId)
	assert.NoError(t, err)
}

func TestAccountRepository_CanNotOpenDuplicateAccount(t *testing.T) {
	store := newInMemoryStore()
	repo := NewAccountRepository(store, 0)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := repo.Open(id, ownerId)
	assert.NoError(t, err)

	err = repo.Open(id, ownerId)
	assert.EqualError(t, err, "account already exists")
}

func TestAccountRepository_CanOpenDistinctAccounts(t *testing.T) {
	store := newInMemoryStore()
	repo := NewAccountRepository(store, 0)

	ownerId := account.NewOwnerId()
	err := repo.Open(account.NewAccountId(), ownerId)
	assert.NoError(t, err)

	err = repo.Open(account.NewAccountId(), ownerId)
	assert.NoError(t, err)
}

func TestAccountRepository_CanNotDepositWhenNoAccountExists(t *testing.T) {
	// given
	store := newInMemoryStore()
	repo := NewAccountRepository(store, 0)

	// when
	id := account.NewAccountId()
	err := repo.Transact(id, func(a *account.Account) error {
		return a.Deposit(42)
	})

	// then
	assert.EqualError(t, err, "aggregate not found")
}

func TestAccountRepository_Deposit(t *testing.T) {
	// given
	store := newInMemoryStore()
	repo := NewAccountRepository(store, 0)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := store.Append(
		[]sequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
		},
		map[account.Id]sequencedEvent{},
	)

	// when
	err = repo.Transact(id, func(a *account.Account) error {
		return a.Deposit(42)
	})

	// then
	assert.NoError(t, err)
	expectEvents(t, store.events, []sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{42, 42}},
	})
}

func TestAccountRepository_Withdraw(t *testing.T) {
	// given
	store := newInMemoryStore()
	repo := NewAccountRepository(store, 0)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := store.Append(
		[]sequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
			{id, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]sequencedEvent{},
	)
	assert.NoError(t, err)

	// when
	err = repo.Transact(id, func(a *account.Account) error {
		return a.Withdraw(2)
	})

	// then
	assert.NoError(t, err)
	expectEvents(t, store.events, []sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{10, 10}},
		{id, 3, account.MoneyWithdrawnEvent{2, 8}},
	})
}

func TestTransferMoney(t *testing.T) {
	// given
	store := newInMemoryStore()
	sourceAccountId := account.NewAccountId()
	sourceOwnerId := account.NewOwnerId()
	err := store.Append(
		[]sequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]sequencedEvent{},
	)
	assert.NoError(t, err)

	targetAccountId := account.NewAccountId()
	targetOwnerId := account.NewOwnerId()
	err = store.Append(
		[]sequencedEvent{
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.Id]sequencedEvent{},
	)
	assert.NoError(t, err)

	repo := NewAccountRepository(store, 0)

	// when
	var transferAmount int64 = 2
	err = repo.BiTransact(sourceAccountId, targetAccountId, func(source, target *account.Account) error {
		err := source.Withdraw(transferAmount)
		if err != nil {
			return err
		}
		return target.Deposit(transferAmount)
	})

	// then
	assert.NoError(t, err)
	expectEvents(t, store.events, []sequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		{sourceAccountId, 3, account.MoneyWithdrawnEvent{2, 8}},
		{targetAccountId, 2, account.MoneyDepositedEvent{2, 2}},
	})
}

func TestTransferMoneyFailsWithInsufficientBalance(t *testing.T) {
	// given
	store := newInMemoryStore()
	sourceAccountId := account.NewAccountId()
	sourceOwnerId := account.NewOwnerId()
	err := store.Append(
		[]sequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]sequencedEvent{},
	)
	assert.NoError(t, err)

	targetAccountId := account.NewAccountId()
	targetOwnerId := account.NewOwnerId()
	err = store.Append(
		[]sequencedEvent{
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.Id]sequencedEvent{},
	)
	assert.NoError(t, err)

	repo := NewAccountRepository(store, 0)

	// when
	var transferAmount int64 = 11
	err = repo.BiTransact(sourceAccountId, targetAccountId, func(source, target *account.Account) error {
		err := source.Withdraw(transferAmount)
		if err != nil {
			return err
		}
		return target.Deposit(transferAmount)
	})

	// then
	assert.EqualError(t, err, "insufficient balance")
	expectEvents(t, store.events, []sequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
	})
}

func TestTransferMoneyFailsWithNonexistentTargetAccount(t *testing.T) {
	// given
	store := newInMemoryStore()
	sourceAccountId := account.NewAccountId()
	sourceOwnerId := account.NewOwnerId()
	err := store.Append(
		[]sequencedEvent{
			{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
			{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.Id]sequencedEvent{},
	)
	assert.NoError(t, err)

	targetAccountId := account.NewAccountId()
	repo := NewAccountRepository(store, 0)

	// when
	var transferAmount int64 = 3
	err = repo.BiTransact(sourceAccountId, targetAccountId, func(source, target *account.Account) error {
		err := source.Withdraw(transferAmount)
		if err != nil {
			return err
		}
		return target.Deposit(transferAmount)
	})

	// then
	assert.EqualError(t, err, "aggregate not found")
	expectEvents(t, store.events, []sequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
}

func expectEvents(t *testing.T, actual, expected []sequencedEvent) {
	assert.Equal(t, len(expected), len(actual), "event counts do not match")
	assert.Equal(t, expected, actual, "events do not match")
}

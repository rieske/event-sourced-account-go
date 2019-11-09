package eventsourcing

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/test"
	"testing"
)

func TestAccountRepository_Open(t *testing.T) {
	store := newInMemoryStore()
	repo := NewAccountRepository(store)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := repo.Open(id, ownerId)
	test.ExpectNoError(t, err)
}

func TestAccountRepository_CanNotOpenDuplicateAccount(t *testing.T) {
	store := newInMemoryStore()
	repo := NewAccountRepository(store)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := repo.Open(id, ownerId)
	test.ExpectNoError(t, err)

	err = repo.Open(id, ownerId)
	test.ExpectError(t, err, "account already exists")
}

func TestAccountRepository_CanOpenDistinctAccounts(t *testing.T) {
	store := newInMemoryStore()
	repo := NewAccountRepository(store)

	ownerId := account.NewOwnerId()
	err := repo.Open(account.NewAccountId(), ownerId)
	test.ExpectNoError(t, err)

	err = repo.Open(account.NewAccountId(), ownerId)
	test.ExpectNoError(t, err)
}

func TestAccountRepository_CanNotDepositWhenNoAccountExists(t *testing.T) {
	// given
	store := newInMemoryStore()
	repo := NewAccountRepository(store)

	// when
	id := account.NewAccountId()
	err := repo.Transact(id, func(a *account.Account) error {
		return a.Deposit(42)
	})

	// then
	test.ExpectError(t, err, "Aggregate not found")
}

func TestAccountRepository_Deposit(t *testing.T) {
	// given
	store := newInMemoryStore()
	repo := NewAccountRepository(store)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := store.Append(
		[]sequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
		},
		map[account.AggregateId]sequencedEvent{},
	)

	// when
	err = repo.Transact(id, func(a *account.Account) error {
		return a.Deposit(42)
	})

	// then
	test.ExpectNoError(t, err)
	expectEvents(t, store.events, []sequencedEvent{
		{id, 1, account.AccountOpenedEvent{id, ownerId}},
		{id, 2, account.MoneyDepositedEvent{42, 42}},
	})
}

func TestAccountRepository_Withdraw(t *testing.T) {
	// given
	store := newInMemoryStore()
	repo := NewAccountRepository(store)

	id := account.NewAccountId()
	ownerId := account.NewOwnerId()
	err := store.Append(
		[]sequencedEvent{
			{id, 1, account.AccountOpenedEvent{id, ownerId}},
			{id, 2, account.MoneyDepositedEvent{10, 10}},
		},
		map[account.AggregateId]sequencedEvent{},
	)
	test.ExpectNoError(t, err)

	// when
	err = repo.Transact(id, func(a *account.Account) error {
		return a.Withdraw(2)
	})

	// then
	test.ExpectNoError(t, err)
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
		map[account.AggregateId]sequencedEvent{},
	)
	test.ExpectNoError(t, err)

	targetAccountId := account.NewAccountId()
	targetOwnerId := account.NewOwnerId()
	err = store.Append(
		[]sequencedEvent{
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.AggregateId]sequencedEvent{},
	)
	test.ExpectNoError(t, err)

	repo := NewAccountRepository(store)

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
	test.ExpectNoError(t, err)
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
		map[account.AggregateId]sequencedEvent{},
	)
	test.ExpectNoError(t, err)

	targetAccountId := account.NewAccountId()
	targetOwnerId := account.NewOwnerId()
	err = store.Append(
		[]sequencedEvent{
			{targetAccountId, 1, account.AccountOpenedEvent{targetAccountId, targetOwnerId}},
		},
		map[account.AggregateId]sequencedEvent{},
	)
	test.ExpectNoError(t, err)

	repo := NewAccountRepository(store)

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
	test.ExpectError(t, err, "insufficient balance")
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
		map[account.AggregateId]sequencedEvent{},
	)
	test.ExpectNoError(t, err)

	targetAccountId := account.NewAccountId()
	repo := NewAccountRepository(store)

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
	test.ExpectError(t, err, "Aggregate not found")
	expectEvents(t, store.events, []sequencedEvent{
		{sourceAccountId, 1, account.AccountOpenedEvent{sourceAccountId, sourceOwnerId}},
		{sourceAccountId, 2, account.MoneyDepositedEvent{10, 10}},
	})
}

func expectEvents(t *testing.T, actual, expected []sequencedEvent) {
	if len(actual) != len(expected) {
		t.Errorf("event counts do not match, expected %v, got %v", len(expected), len(actual))
		return
	}

	for i := range actual {
		if actual[i] != expected[i] {
			t.Errorf("Event at index %v does not match, expected %v, got %v", i, expected, actual)
		}
	}
}

package account

import "testing"

func TestAccountRepository_Open(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := NewAccountId()
	ownerId := NewOwnerId()
	err := repo.Open(id, ownerId)
	expectNoError(t, err)
}

func TestAccountRepository_CanNotOpenDuplicateAccount(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := NewAccountId()
	ownerId := NewOwnerId()
	err := repo.Open(id, ownerId)
	expectNoError(t, err)

	err = repo.Open(id, ownerId)
	expectError(t, err, "account already exists")
}

func TestAccountRepository_CanOpenDistinctAccounts(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	ownerId := NewOwnerId()
	err := repo.Open(NewAccountId(), ownerId)
	expectNoError(t, err)

	err = repo.Open(NewAccountId(), ownerId)
	expectNoError(t, err)
}

func TestAccountRepository_CanNotDepositWhenNoAccountExists(t *testing.T) {
	// given
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	// when
	id := NewAccountId()
	err := repo.Transact(id, func(a *account) (Event, error) {
		return a.Deposit(42)
	})

	// then
	expectError(t, err, "Aggregate not found")
}

func TestAccountRepository_Deposit(t *testing.T) {
	// given
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := NewAccountId()
	ownerId := NewOwnerId()
	err := store.Append([]sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
	})

	// when
	err = repo.Transact(id, func(a *account) (Event, error) {
		return a.Deposit(42)
	})

	// then
	expectNoError(t, err)
	expectEvents(t, store.events, []sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
		{id, 2, MoneyDepositedEvent{42, 42}},
	})
}

func TestAccountRepository_Withdraw(t *testing.T) {
	// given
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := NewAccountId()
	ownerId := NewOwnerId()
	err := store.Append([]sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
		{id, 2, MoneyDepositedEvent{10, 10}},
	})
	expectNoError(t, err)

	// when
	err = repo.Transact(id, func(a *account) (Event, error) {
		return a.Withdraw(2)
	})

	// then
	expectNoError(t, err)
	expectEvents(t, store.events, []sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
		{id, 2, MoneyDepositedEvent{10, 10}},
		{id, 3, MoneyWithdrawnEvent{2, 8}},
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

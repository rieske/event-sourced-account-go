package account

import "testing"

func TestAccountRepository_Open(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := AggregateId{1}
	ownerId := OwnerId{2}
	err := repo.Open(id, ownerId)
	expectNoError(t, err)
}

func TestAccountRepository_CanNotOpenDuplicateAccount(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := AggregateId{1}
	ownerId := OwnerId{2}
	err := repo.Open(id, ownerId)
	expectNoError(t, err)

	err = repo.Open(id, ownerId)
	expectError(t, err, "account already exists")
}

func TestAccountRepository_CanOpenDistinctAccounts(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	ownerId := OwnerId{2}
	err := repo.Open(AggregateId{1}, ownerId)
	expectNoError(t, err)

	err = repo.Open(AggregateId{2}, ownerId)
	expectNoError(t, err)
}

func TestAccountRepository_CanNotDepositWhenNoAccountExists(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := AggregateId{1}
	err := repo.Transact(id, func(a *account) (Event, error) {
		return a.Deposit(42)
	})
	expectError(t, err, "Aggregate not found")
}

func TestAccountRepository_Deposit(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := AggregateId{1}
	ownerId := OwnerId{2}
	err := store.Append([]sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
	})

	err = repo.Transact(id, func(a *account) (Event, error) {
		return a.Deposit(42)
	})
	expectNoError(t, err)
}

func TestAccountRepository_Withdraw(t *testing.T) {
	store := inmemoryEeventstore{}
	repo := NewAccountRepository(&store)

	id := AggregateId{1}
	ownerId := OwnerId{2}
	err := store.Append([]sequencedEvent{
		{id, 1, AccountOpenedEvent{id, ownerId}},
		{id, 2, MoneyDepositedEvent{10, 10}},
	})
	expectNoError(t, err)

	err = repo.Transact(id, func(a *account) (Event, error) {
		return a.Withdraw(5)
	})
	expectNoError(t, err)
}

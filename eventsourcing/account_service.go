package eventsourcing

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
)

type AccountService struct {
	repo *repository
}

func NewAccountService(store EventStore, snapshotFrequency int) *AccountService {
	repo := NewAccountRepository(store, snapshotFrequency)
	return &AccountService{repo: repo}
}

func (s AccountService) OpenAccount(id account.ID, ownerID account.OwnerID) error {
	return s.repo.create(id, func(a *account.Account) error {
		return a.Open(id, ownerID)
	})
}

func (s AccountService) Deposit(id account.ID, txId uuid.UUID, amount int64) error {
	return s.repo.transact(id, txId, func(a *account.Account) error {
		return a.Deposit(amount)
	})
}

func (s AccountService) Withdraw(id account.ID, txId uuid.UUID, amount int64) error {
	return s.repo.transact(id, txId, func(a *account.Account) error {
		return a.Withdraw(amount)
	})
}

func (s AccountService) CloseAccount(id account.ID) error {
	return s.repo.transact(id, uuid.New(), func(a *account.Account) error {
		return a.Close()
	})
}

func (s AccountService) Transfer(sourceAccountId, targetAccountId account.ID, txId uuid.UUID, amount int64) error {
	return s.repo.biTransact(sourceAccountId, targetAccountId, txId, func(source *account.Account, target *account.Account) error {
		err := source.Withdraw(amount)
		if err != nil {
			return err
		}
		return target.Deposit(amount)
	})
}

func (s AccountService) QueryAccount(id account.ID) (*account.Snapshot, error) {
	return s.repo.query(id)
}

func (s AccountService) Events(id account.ID) ([]eventstore.SequencedEvent, error) {
	return s.repo.store.Events(id, 0)
}

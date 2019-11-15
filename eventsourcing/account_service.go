package eventsourcing

import (
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
)

type accountService struct {
	repo repository
}

func (s accountService) OpenAccount(id account.Id, ownerId account.OwnerId) error {
	return s.repo.create(id, func(a *account.Account) error {
		return a.Open(id, ownerId)
	})
}

func (s accountService) Deposit(id account.Id, txId uuid.UUID, amount int64) error {
	return s.repo.transact(id, txId, func(a *account.Account) error {
		return a.Deposit(amount)
	})
}

func (s accountService) Withdraw(id account.Id, txId uuid.UUID, amount int64) error {
	return s.repo.transact(id, txId, func(a *account.Account) error {
		return a.Withdraw(amount)
	})
}

func (s accountService) CloseAccount(id account.Id) error {
	return s.repo.transact(id, uuid.New(), func(a *account.Account) error {
		return a.Close()
	})
}

func (s accountService) Transfer(sourceAccountId, targetAccountId account.Id, txId uuid.UUID, amount int64) error {
	return s.repo.biTransact(sourceAccountId, targetAccountId, txId, func(source *account.Account, target *account.Account) error {
		err := source.Withdraw(amount)
		if err != nil {
			return err
		}
		return target.Deposit(amount)
	})
}

func (s accountService) QueryAccount(id account.Id) (*account.Snapshot, error) {
	return s.repo.query(id)
}

func (s accountService) Events(id account.Id) ([]eventstore.SequencedEvent, error) {
	return s.repo.store.Events(id, 0)
}

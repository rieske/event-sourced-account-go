package eventsourcing

import (
	"context"

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

func (s AccountService) OpenAccount(ctx context.Context, id account.ID, ownerID account.OwnerID) error {
	return s.repo.create(ctx, id, func(a *account.Account) error {
		return a.Open(id, ownerID)
	})
}

func (s AccountService) Deposit(ctx context.Context, id account.ID, txId uuid.UUID, amount int64) error {
	return retryOnConcurrentModification(func() error {
		return s.repo.transact(ctx, id, txId, func(a *account.Account) error {
			return a.Deposit(amount)
		})
	})
}

func (s AccountService) Withdraw(ctx context.Context, id account.ID, txId uuid.UUID, amount int64) error {
	return retryOnConcurrentModification(func() error {
		return s.repo.transact(ctx, id, txId, func(a *account.Account) error {
			return a.Withdraw(amount)
		})
	})
}

func (s AccountService) CloseAccount(ctx context.Context, id account.ID) error {
	return s.repo.transact(ctx, id, uuid.New(), func(a *account.Account) error {
		return a.Close()
	})
}

func (s AccountService) Transfer(ctx context.Context, sourceAccountId, targetAccountId account.ID, txId uuid.UUID, amount int64) error {
	return retryOnConcurrentModification(func() error {
		return s.repo.biTransact(ctx, sourceAccountId, targetAccountId, txId, func(source *account.Account, target *account.Account) error {
			if err := source.Withdraw(amount); err != nil {
				return err
			}
			return target.Deposit(amount)
		})
	})
}

func (s AccountService) QueryAccount(ctx context.Context, id account.ID) (*account.Snapshot, error) {
	return s.repo.query(ctx, id)
}

func (s AccountService) Events(ctx context.Context, id account.ID) ([]eventstore.SequencedEvent, error) {
	return s.repo.store.Events(ctx, id, 0)
}

func retryOnConcurrentModification(fn func() error) error {
	var err error
	for try := 0; try < 3; try++ {
		err = fn()
		if err != account.ConcurrentModification {
			return err
		}
	}
	return err
}

package eventsourcing

import "github.com/rieske/event-sourced-account-go/account"

type accountService struct {
	repo repository
}

func (s accountService) OpenAccount(id account.Id, ownerId account.OwnerId) error {
	return s.repo.create(id, func(a *account.Account) error {
		return a.Open(id, ownerId)
	})
}

func (s accountService) Deposit(id account.Id, amount int64) error {
	return s.repo.transact(id, func(a *account.Account) error {
		return a.Deposit(amount)
	})
}

func (s accountService) Withdraw(id account.Id, amount int64) error {
	return s.repo.transact(id, func(a *account.Account) error {
		return a.Withdraw(amount)
	})
}

func (s accountService) CloseAccount(id account.Id) error {
	return s.repo.transact(id, func(a *account.Account) error {
		return a.Close()
	})
}

func (s accountService) Transfer(sourceAccountId account.Id, targetAccountId account.Id, amount int64) error {
	return s.repo.biTransact(sourceAccountId, targetAccountId, func(source *account.Account, target *account.Account) error {
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

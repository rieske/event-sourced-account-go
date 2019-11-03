package account

import (
	"errors"
	"fmt"
)

type AggregateId UUID
type OwnerId UUID

type account struct {
	id      *AggregateId
	ownerId *OwnerId
	balance int64
	open    bool
}

type Snapshot struct {
	id      AggregateId
	ownerId OwnerId
	balance int64
	open    bool
}

func NewAccount() *account {
	return &account{}
}

func (a account) Id() AggregateId {
	return *a.id
}

func (a *account) Snapshot() (*Snapshot, error) {
	if !a.open {
		return nil, errors.New("account not open")
	}

	return &Snapshot{*a.id, *a.ownerId, a.balance, a.open}, nil
}

func (a *account) Open(accountId AggregateId, ownerId OwnerId) (Event, error) {
	if a.id != nil || a.ownerId != nil {
		return nil, errors.New("account already open")
	}

	event := AccountOpenedEvent{accountId, ownerId}
	a.applyAccountOpened(event)
	return event, nil
}

func (a *account) Deposit(amount int64) (Event, error) {
	if amount < 0 {
		return nil, errors.New("Can not deposit negative amount")
	}
	if !a.open {
		return nil, errors.New("Account not open")
	}
	if amount == 0 {
		return nil, nil
	}

	event := MoneyDepositedEvent{amount, a.balance + amount}
	a.applyMoneyDeposited(event)
	return event, nil
}

func (a *account) Withdraw(amount int64) (Event, error) {
	if amount < 0 {
		return nil, errors.New("Can not withdraw negative amount")
	}
	if !a.open {
		return nil, errors.New("Account not open")
	}
	if amount > a.balance {
		return nil, errors.New("Insufficient balance")
	}
	if amount == 0 {
		return nil, nil
	}

	event := MoneyWithdrawnEvent{amount, a.balance - amount}
	a.applyMoneyWithdrawn(event)
	return event, nil
}

func (a *account) Close() (Event, error) {
	if a.balance != 0 {
		return nil, errors.New("Balance outstanding")
	}

	event := AccountClosedEvent{}
	a.applyAccountClosed(event)
	return event, nil
}

func (a *account) applyAccountOpened(event AccountOpenedEvent) {
	a.id = &event.accountId
	a.ownerId = &event.ownerId
	a.balance = 0
	a.open = true
}

func (a *account) applyMoneyDeposited(event MoneyDepositedEvent) {
	a.balance = event.balance
}

func (a *account) applyMoneyWithdrawn(event MoneyWithdrawnEvent) {
	a.balance = event.balance
}

func (a *account) applyAccountClosed(event AccountClosedEvent) {
	a.open = false
}

type Event interface {
	apply(account *account)
	//Serialize() []byte
}

type AccountOpenedEvent struct {
	accountId AggregateId
	ownerId   OwnerId
}

func (e AccountOpenedEvent) String() string {
	return fmt.Sprintf("AccountOpenedEvent{ownerId: %s}", e.ownerId)
}

func (e AccountOpenedEvent) apply(account *account) {
	account.applyAccountOpened(e)
}

type MoneyDepositedEvent struct {
	amountDeposited int64
	balance         int64
}

func (e MoneyDepositedEvent) apply(account *account) {
	account.applyMoneyDeposited(e)
}

type MoneyWithdrawnEvent struct {
	amountWithdrawn int64
	balance         int64
}

func (e MoneyWithdrawnEvent) apply(account *account) {
	account.applyMoneyWithdrawn(e)
}

type AccountClosedEvent struct {
}

func (e AccountClosedEvent) apply(account *account) {
	account.applyAccountClosed(e)
}

/*func (e AccountOpenedEvent) Serialize() []byte {
	return e.ownerId
}*/

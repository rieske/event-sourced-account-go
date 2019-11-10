package account

import (
	"errors"
	"github.com/google/uuid"
)

type OwnerId uuid.UUID

type AggregateId uuid.UUID

type Aggregate interface {
	Snapshot() Snapshot

	applySnapshot(snapshot Snapshot)
	applyAccountOpened(event AccountOpenedEvent)
	applyMoneyDeposited(event MoneyDepositedEvent)
	applyMoneyWithdrawn(event MoneyWithdrawnEvent)
	applyAccountClosed(event AccountClosedEvent)
}

type EventStream interface {
	Append(e Event, a Aggregate, id AggregateId)
}

type Account struct {
	es      EventStream
	id      AggregateId
	ownerId OwnerId
	balance int64
	open    bool
}

func NewAccountId() AggregateId {
	return AggregateId(uuid.New())
}

func NewOwnerId() OwnerId {
	return OwnerId(uuid.New())
}

func NewAccount(es EventStream) *Account {
	return &Account{es: es}
}

func (a Account) Id() AggregateId {
	return a.id
}

func (a *Account) Snapshot() Snapshot {
	return Snapshot{a.id, a.ownerId, a.balance, a.open}
}

func (a *Account) Open(accountId AggregateId, ownerId OwnerId) error {
	if a.open {
		return errors.New("account already open")
	}

	event := AccountOpenedEvent{accountId, ownerId}
	a.es.Append(event, a, accountId)
	return nil
}

func (a *Account) Deposit(amount int64) error {
	if amount < 0 {
		return errors.New("can not deposit negative amount")
	}
	if !a.open {
		return errors.New("account not open")
	}
	if amount == 0 {
		return nil
	}

	event := MoneyDepositedEvent{amount, a.balance + amount}
	a.es.Append(event, a, a.id)
	return nil
}

func (a *Account) Withdraw(amount int64) error {
	if amount < 0 {
		return errors.New("can not withdraw negative amount")
	}
	if !a.open {
		return errors.New("account not open")
	}
	if amount > a.balance {
		return errors.New("insufficient balance")
	}
	if amount == 0 {
		return nil
	}

	event := MoneyWithdrawnEvent{amount, a.balance - amount}
	a.es.Append(event, a, a.id)
	return nil
}

func (a *Account) Close() error {
	if a.balance != 0 {
		return errors.New("balance outstanding")
	}

	event := AccountClosedEvent{}
	a.es.Append(event, a, a.id)
	return nil
}

func (a *Account) applySnapshot(snapshot Snapshot) {
	a.id = snapshot.Id
	a.ownerId = snapshot.OwnerId
	a.balance = snapshot.Balance
	a.open = snapshot.Open
}

func (a *Account) applyAccountOpened(event AccountOpenedEvent) {
	a.id = event.AccountId
	a.ownerId = event.OwnerId
	a.balance = 0
	a.open = true
}

func (a *Account) applyMoneyDeposited(event MoneyDepositedEvent) {
	a.balance = event.Balance
}

func (a *Account) applyMoneyWithdrawn(event MoneyWithdrawnEvent) {
	a.balance = event.Balance
}

func (a *Account) applyAccountClosed(event AccountClosedEvent) {
	a.open = false
}

type Snapshot struct {
	Id      AggregateId
	OwnerId OwnerId
	Balance int64
	Open    bool
}

func (s Snapshot) Apply(a Aggregate) {
	a.applySnapshot(s)
}

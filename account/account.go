package account

import (
	"errors"
	"github.com/google/uuid"
)

type Id struct {
	uuid.UUID
}
type OwnerId struct {
	uuid.UUID
}

type EventAppender interface {
	Append(e Event, a *Account, id Id)
}

type Account struct {
	eventAppender EventAppender
	id            Id
	ownerId       OwnerId
	balance       int64
	open          bool
}

func NewId() Id {
	return Id{UUID: uuid.New()}
}

func NewOwnerId() OwnerId {
	return OwnerId{UUID: uuid.New()}
}

func New(es EventAppender) *Account {
	return &Account{eventAppender: es}
}

func (a Account) Id() Id {
	return a.id
}

func (a Account) Snapshot() Snapshot {
	return Snapshot{Id: a.id, OwnerId: a.ownerId, Balance: a.balance, Open: a.open}
}

func (a *Account) Open(accountId Id, ownerId OwnerId) error {
	if a.open {
		return errors.New("account already open")
	}

	event := AccountOpenedEvent{AccountId: accountId, OwnerId: ownerId}
	a.eventAppender.Append(event, a, accountId)
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

	event := MoneyDepositedEvent{AmountDeposited: amount, Balance: a.balance + amount}
	a.eventAppender.Append(event, a, a.id)
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

	event := MoneyWithdrawnEvent{AmountWithdrawn: amount, Balance: a.balance - amount}
	a.eventAppender.Append(event, a, a.id)
	return nil
}

func (a *Account) Close() error {
	if a.balance != 0 {
		return errors.New("balance outstanding")
	}

	event := AccountClosedEvent{}
	a.eventAppender.Append(event, a, a.id)
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
	Id      Id
	OwnerId OwnerId
	Balance int64
	Open    bool
}

func (s Snapshot) Apply(a *Account) {
	a.applySnapshot(s)
}

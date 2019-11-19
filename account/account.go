package account

import (
	"github.com/google/uuid"
)

type ID struct {
	uuid.UUID
}
type OwnerID struct {
	uuid.UUID
}

type EventAppender interface {
	Append(e Event, a *Account, id ID)
}

type Account struct {
	eventAppender EventAppender
	id            ID
	ownerID       OwnerID
	balance       int64
	open          bool
}

func NewID() ID {
	return ID{UUID: uuid.New()}
}

func NewOwnerID() OwnerID {
	return OwnerID{UUID: uuid.New()}
}

func New(es EventAppender) *Account {
	return &Account{eventAppender: es}
}

func (a Account) ID() ID {
	return a.id
}

func (a Account) Snapshot() Snapshot {
	return Snapshot{ID: a.id, OwnerID: a.ownerID, Balance: a.balance, Open: a.open}
}

func (a *Account) Open(accountID ID, ownerID OwnerID) error {
	if a.open {
		return AlreadyOpen
	}

	event := AccountOpenedEvent{AccountID: accountID, OwnerID: ownerID}
	a.eventAppender.Append(event, a, accountID)
	return nil
}

func (a *Account) Deposit(amount int64) error {
	if amount < 0 {
		return NegativeDeposit
	}
	if !a.open {
		return NotOpen
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
		return NegativeWithdrawal
	}
	if !a.open {
		return NotOpen
	}
	if amount > a.balance {
		return InsufficientBalance
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
		return BalanceOutstanding
	}

	event := AccountClosedEvent{}
	a.eventAppender.Append(event, a, a.id)
	return nil
}

func (a *Account) applySnapshot(snapshot Snapshot) {
	a.id = snapshot.ID
	a.ownerID = snapshot.OwnerID
	a.balance = snapshot.Balance
	a.open = snapshot.Open
}

func (a *Account) applyAccountOpened(event AccountOpenedEvent) {
	a.id = event.AccountID
	a.ownerID = event.OwnerID
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
	ID      ID
	OwnerID OwnerID
	Balance int64
	Open    bool
}

func (s Snapshot) Apply(a *Account) {
	a.applySnapshot(s)
}

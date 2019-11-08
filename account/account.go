package account

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

type AggregateId uuid.UUID
type OwnerId uuid.UUID

type account struct {
	es      eventStream
	id      AggregateId
	ownerId OwnerId
	balance int64
	open    bool
}

type Snapshot struct {
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

func newAccount(es eventStream) account {
	return account{es: es}
}

func (a account) Id() AggregateId {
	return a.id
}

func (a *account) Snapshot() Snapshot {
	return Snapshot{a.id, a.ownerId, a.balance, a.open}
}

func (a *account) Open(accountId AggregateId, ownerId OwnerId) error {
	if a.open {
		return errors.New("account already open")
	}

	event := AccountOpenedEvent{accountId, ownerId}
	a.es.append(event, a, accountId)
	return nil
}

func (a *account) Deposit(amount int64) error {
	if amount < 0 {
		return errors.New("Can not deposit negative amount")
	}
	if !a.open {
		return errors.New("Account not open")
	}
	if amount == 0 {
		return nil
	}

	event := MoneyDepositedEvent{amount, a.balance + amount}
	a.es.append(event, a, a.id)
	return nil
}

func (a *account) Withdraw(amount int64) error {
	if amount < 0 {
		return errors.New("Can not withdraw negative amount")
	}
	if !a.open {
		return errors.New("Account not open")
	}
	if amount > a.balance {
		return errors.New("Insufficient balance")
	}
	if amount == 0 {
		return nil
	}

	event := MoneyWithdrawnEvent{amount, a.balance - amount}
	a.es.append(event, a, a.id)
	return nil
}

func (a *account) Close() error {
	if a.balance != 0 {
		return errors.New("Balance outstanding")
	}

	event := AccountClosedEvent{}
	a.es.append(event, a, a.id)
	return nil
}

func (a *account) applyAccountOpened(event AccountOpenedEvent) {
	a.id = event.accountId
	a.ownerId = event.ownerId
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

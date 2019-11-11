package account

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type immediateEventStream struct{}

func (s *immediateEventStream) Append(e Event, a *Account, id Id) {
	e.Apply(a)
}

func TestOpenAccount(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	err := a.Open(accountId, ownerId)
	if err != nil {
		t.Error(err)
	}

	if a.id != accountId {
		t.Error("Account Id should be set")
	}
	if a.ownerId != ownerId {
		t.Error("owner Id should be set")
	}
	if a.open != true {
		t.Error("Account should be open")
	}
	expectBalance(t, a, 0)
}

func TestOpenAccountAlreadyOpen(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)
	err := a.Open(accountId, ownerId)
	assert.EqualError(t, err, "account already open")
}

func TestDeposit(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(42)

	assert.NoError(t, err)
	expectBalance(t, a, 42)
}

func TestDepositAccumulatesBalance(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	_ = a.Deposit(1)
	_ = a.Deposit(2)

	expectBalance(t, a, 3)
}

func TestCanNotDepositNegativeAmount(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(-1)

	assert.EqualError(t, err, "can not deposit negative amount")
	expectBalance(t, a, 0)
}

func TestZeroDepositShouldNotEmitEvent(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(0)

	assert.NoError(t, err)
}

func TestRequireOpenAccountForDeposit(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	err := a.Deposit(0)

	assert.EqualError(t, err, "account not open")
}

func TestWithdrawal(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)
	_ = a.Deposit(10)

	err := a.Withdraw(5)

	assert.NoError(t, err)
	expectBalance(t, a, 5)
}

func TestCanNotWithdrawWhenBalanceInsufficient(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(5)

	assert.EqualError(t, err, "insufficient balance")
}

func TestCanNotWithdrawNegativeAmount(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(-1)

	assert.EqualError(t, err, "can not withdraw negative amount")
}

func TestZeroWithdrawalShouldNotEmitEvent(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(0)

	assert.NoError(t, err)
}

func TestRequireOpenAccountForWithdrawal(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	err := a.Withdraw(0)

	assert.EqualError(t, err, "account not open")
}

func TestCloseAccount(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Close()

	assert.NoError(t, err)
	if a.open != false {
		t.Error("Account should be closed")
	}
}

func TestCanNotCloseAccountWithOutstandingBalance(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)
	_ = a.Deposit(10)

	err := a.Close()

	assert.EqualError(t, err, "balance outstanding")
}

func TestApplyEvents(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()

	events := []Event{
		AccountOpenedEvent{accountId, ownerId},
		MoneyDepositedEvent{1, 1},
		MoneyDepositedEvent{2, 3},
	}

	for _, e := range events {
		e.Apply(a)
	}

	if a.id != accountId {
		t.Error("Account Id should be set")
	}
	if a.ownerId != ownerId {
		t.Error("owner Id should be set")
	}
	if a.open != true {
		t.Error("Account should be Open")
	}
	expectBalance(t, a, 3)
}

func expectBalance(t *testing.T, a *Account, balance int64) {
	if a.balance != balance {
		t.Errorf("Balance should be %d, got %d", balance, a.balance)
	}
}

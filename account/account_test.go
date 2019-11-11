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

	accountId, ownerId := NewAccountId(), NewOwnerId()
	err := a.Open(accountId, ownerId)

	assert.NoError(t, err)
	assert.Equal(t, accountId, a.id)
	assert.Equal(t, ownerId, a.ownerId)
	assert.True(t, a.open)
	assert.Zero(t, a.balance)
}

func TestOpenAccountAlreadyOpen(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)
	err := a.Open(accountId, ownerId)
	assert.EqualError(t, err, "account already open")
}

func TestDeposit(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(42)

	assert.NoError(t, err)
	assert.Equal(t, int64(42), a.balance)
}

func TestDepositAccumulatesBalance(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)

	_ = a.Deposit(1)
	_ = a.Deposit(2)

	assert.Equal(t, int64(3), a.balance)
}

func TestCanNotDepositNegativeAmount(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(-1)

	assert.EqualError(t, err, "can not deposit negative amount")
	assert.Zero(t, a.balance)
}

func TestZeroDepositShouldNotEmitEvent(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
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

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)
	_ = a.Deposit(10)

	err := a.Withdraw(5)

	assert.NoError(t, err)
	assert.Equal(t, int64(5), a.balance)
}

func TestCanNotWithdrawWhenBalanceInsufficient(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(5)

	assert.EqualError(t, err, "insufficient balance")
}

func TestCanNotWithdrawNegativeAmount(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(-1)

	assert.EqualError(t, err, "can not withdraw negative amount")
}

func TestZeroWithdrawalShouldNotEmitEvent(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(0)

	assert.NoError(t, err)
}

func TestRequireOpenAccountForWithdrawal(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	err := a.Withdraw(1)

	assert.EqualError(t, err, "account not open")
}

func TestCloseAccount(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Close()

	assert.NoError(t, err)
	assert.False(t, a.open)
}

func TestCanNotCloseAccountWithOutstandingBalance(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	_ = a.Open(accountId, ownerId)
	_ = a.Deposit(10)

	err := a.Close()

	assert.EqualError(t, err, "balance outstanding")
}

func TestApplyEvents(t *testing.T) {
	a := NewAccount(&immediateEventStream{})

	accountId, ownerId := NewAccountId(), NewOwnerId()
	events := []Event{
		AccountOpenedEvent{accountId, ownerId},
		MoneyDepositedEvent{1, 1},
		MoneyDepositedEvent{2, 3},
	}

	for _, e := range events {
		e.Apply(a)
	}

	assert.Equal(t, accountId, a.id)
	assert.Equal(t, ownerId, a.ownerId)
	assert.True(t, a.open)
	assert.Equal(t, int64(3), a.balance)
}

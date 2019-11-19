package account_test

import (
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
	"testing"
)

type immediateEventStream struct{}

func (s *immediateEventStream) Append(e account.Event, a *account.Account, id account.ID) {
	e.Apply(a)
}

func newAccount() *account.Account {
	return account.New(&immediateEventStream{})
}

func TestOpenAccount(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	err := a.Open(accountID, ownerID)

	assert.NoError(t, err)
	snapshot := a.Snapshot()
	assert.Equal(t, accountID, snapshot.ID)
	assert.Equal(t, ownerID, snapshot.OwnerID)
	assert.True(t, snapshot.Open)
	assert.Zero(t, snapshot.Balance)
}

func TestOpenAccountAlreadyOpen(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)
	err := a.Open(accountID, ownerID)
	assert.EqualError(t, err, "account already open")
}

func TestDeposit(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	err := a.Deposit(42)

	assert.NoError(t, err)
	snapshot := a.Snapshot()
	assert.Equal(t, int64(42), snapshot.Balance)
}

func TestDepositAccumulatesBalance(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	_ = a.Deposit(1)
	_ = a.Deposit(2)

	snapshot := a.Snapshot()
	assert.Equal(t, int64(3), snapshot.Balance)
}

func TestCanNotDepositNegativeAmount(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	err := a.Deposit(-1)

	assert.EqualError(t, err, "can not deposit negative amount")
	snapshot := a.Snapshot()
	assert.Zero(t, snapshot.Balance)
}

func TestZeroDepositShouldNotEmitEvent(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	err := a.Deposit(0)

	assert.NoError(t, err)
}

func TestRequireOpenAccountForDeposit(t *testing.T) {
	a := newAccount()

	err := a.Deposit(0)

	assert.EqualError(t, err, "account not open")
}

func TestWithdrawal(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)
	_ = a.Deposit(10)

	err := a.Withdraw(5)

	assert.NoError(t, err)
	snapshot := a.Snapshot()
	assert.Equal(t, int64(5), snapshot.Balance)
}

func TestCanNotWithdrawWhenBalanceInsufficient(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	err := a.Withdraw(5)

	assert.EqualError(t, err, "insufficient balance")
}

func TestCanNotWithdrawNegativeAmount(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	err := a.Withdraw(-1)

	assert.EqualError(t, err, "can not withdraw negative amount")
}

func TestZeroWithdrawalShouldNotEmitEvent(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	err := a.Withdraw(0)

	assert.NoError(t, err)
}

func TestRequireOpenAccountForWithdrawal(t *testing.T) {
	a := newAccount()

	err := a.Withdraw(1)

	assert.EqualError(t, err, "account not open")
}

func TestCloseAccount(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)

	err := a.Close()

	assert.NoError(t, err)
	snapshot := a.Snapshot()
	assert.False(t, snapshot.Open)
}

func TestCanNotCloseAccountWithOutstandingBalance(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	_ = a.Open(accountID, ownerID)
	_ = a.Deposit(10)

	err := a.Close()

	assert.EqualError(t, err, "balance outstanding")
}

func TestApplyEvents(t *testing.T) {
	a := newAccount()

	accountID, ownerID := account.NewID(), account.NewOwnerID()
	events := []account.Event{
		account.AccountOpenedEvent{accountID, ownerID},
		account.MoneyDepositedEvent{1, 1},
		account.MoneyDepositedEvent{2, 3},
	}

	for _, e := range events {
		e.Apply(a)
	}

	snapshot := a.Snapshot()
	assert.Equal(t, accountID, snapshot.ID)
	assert.Equal(t, ownerID, snapshot.OwnerID)
	assert.True(t, snapshot.Open)
	assert.Equal(t, int64(3), snapshot.Balance)
}

package account

import (
	"testing"
)

type immediateEventStream struct{}

func (s *immediateEventStream) append(e Event, a *account, id AggregateId) {
	e.apply(a)
}

func TestOpenAccount(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	err := a.Open(accountId, ownerId)
	if err != nil {
		t.Error(err)
	}

	if a.id != accountId {
		t.Error("account id should be set")
	}
	if a.ownerId != ownerId {
		t.Error("owner id should be set")
	}
	if a.open != true {
		t.Error("account should be open")
	}
	expectBalance(t, a, 0)
}

func TestOpenAccountAlreadyOpen(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)
	err := a.Open(accountId, ownerId)
	expectError(t, err, "account already open")
}

func TestDeposit(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(42)

	expectNoError(t, err)
	expectBalance(t, a, 42)
}

func TestDepositAccumulatesBalance(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	_ = a.Deposit(1)
	_ = a.Deposit(2)

	expectBalance(t, a, 3)
}

func TestCanNotDepositNegativeAmount(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(-1)

	expectError(t, err, "Can not deposit negative amount")
	expectBalance(t, a, 0)
}

func TestZeroDepositShouldNotEmitEvent(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Deposit(0)

	expectNoError(t, err)
}

func TestRequireOpenAccountForDeposit(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	err := a.Deposit(0)

	expectError(t, err, "Account not open")
}

func TestWithdrawal(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)
	_ = a.Deposit(10)

	err := a.Withdraw(5)

	expectNoError(t, err)
	expectBalance(t, a, 5)
}

func TestCanNotWithdrawWhenBalanceInsufficient(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(5)

	expectError(t, err, "Insufficient balance")
}

func TestCanNotWithdrawNegativeAmount(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(-1)

	expectError(t, err, "Can not withdraw negative amount")
}

func TestZeroWithdrawalShouldNotEmitEvent(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Withdraw(0)

	expectNoError(t, err)
}

func TestRequireOpenAccountForWithdrawal(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	err := a.Withdraw(0)

	expectError(t, err, "Account not open")
}

func TestCloseAccount(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)

	err := a.Close()

	expectNoError(t, err)
	if a.open != false {
		t.Error("account should be closed")
	}
}

func TestCanNotCloseAccountWithOutstandingBalance(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()
	_ = a.Open(accountId, ownerId)
	_ = a.Deposit(10)

	err := a.Close()

	expectError(t, err, "Balance outstanding")
}

func TestApplyEvents(t *testing.T) {
	a := newAccount(&immediateEventStream{})

	accountId := NewAccountId()
	ownerId := NewOwnerId()

	events := []Event{
		AccountOpenedEvent{accountId, ownerId},
		MoneyDepositedEvent{1, 1},
		MoneyDepositedEvent{2, 3},
	}

	for _, e := range events {
		e.apply(&a)
	}

	if a.id != accountId {
		t.Error("account id should be set")
	}
	if a.ownerId != ownerId {
		t.Error("owner id should be set")
	}
	if a.open != true {
		t.Error("account should be open")
	}
	expectBalance(t, a, 3)
}

func expectError(t *testing.T, err error, message string) {
	if err == nil || err.Error() != message {
		t.Errorf("error expected - %s", message)
	}
}

func expectNoError(t *testing.T, err error) {
	if err != nil {
		t.Error("no error expected, got:", err)
	}
}

func expectBalance(t *testing.T, a account, balance int64) {
	if a.balance != balance {
		t.Errorf("balance should be %d, got %d", balance, a.balance)
	}
}

package account

import "errors"

var (
	Exists              = errors.New("account already exists")
	AlreadyOpen         = errors.New("account already open")
	NotOpen             = errors.New("account not open")
	NegativeDeposit     = errors.New("can not deposit negative amount")
	NegativeWithdrawal  = errors.New("can not withdraw negative amount")
	InsufficientBalance = errors.New("insufficient balance")
	BalanceOutstanding  = errors.New("balance outstanding")
)

package account

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	Exists                 Error = "account already exists"
	NotFound               Error = "account not found"
	AlreadyOpen            Error = "account already open"
	NotOpen                Error = "account not open"
	NegativeDeposit        Error = "can not deposit negative amount"
	NegativeWithdrawal     Error = "can not withdraw negative amount"
	InsufficientBalance    Error = "insufficient balance"
	BalanceOutstanding     Error = "balance outstanding"
	ConcurrentModification Error = "concurrent modification error"
)

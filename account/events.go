package account

type Event interface {
	Apply(account Aggregate)
	//Serialize() []byte
}

type AccountOpenedEvent struct {
	AccountId Id
	OwnerId   OwnerId
}

func (e AccountOpenedEvent) Apply(account Aggregate) {
	account.applyAccountOpened(e)
}

type MoneyDepositedEvent struct {
	AmountDeposited int64
	Balance         int64
}

func (e MoneyDepositedEvent) Apply(account Aggregate) {
	account.applyMoneyDeposited(e)
}

type MoneyWithdrawnEvent struct {
	AmountWithdrawn int64
	Balance         int64
}

func (e MoneyWithdrawnEvent) Apply(account Aggregate) {
	account.applyMoneyWithdrawn(e)
}

type AccountClosedEvent struct {
}

func (e AccountClosedEvent) Apply(account Aggregate) {
	account.applyAccountClosed(e)
}

/*func (e AccountOpenedEvent) Serialize() []byte {
	return e.OwnerId
}*/

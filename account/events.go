package account

type Event interface {
	Apply(account *Account)
}

type Snapshot struct {
	ID      ID      `json:"accountId"`
	OwnerID OwnerID `json:"ownerId"`
	Balance int64   `json:"balance"`
	Open    bool    `json:"open"`
}

func (s Snapshot) Apply(a *Account) {
	a.applySnapshot(s)
}

type AccountOpenedEvent struct {
	AccountID ID      `json:"accountId"`
	OwnerID   OwnerID `json:"ownerId"`
}

func (e AccountOpenedEvent) Apply(account *Account) {
	account.applyAccountOpened(e)
}

type MoneyDepositedEvent struct {
	AmountDeposited int64 `json:"amountDeposited"`
	Balance         int64 `json:"balance"`
}

func (e MoneyDepositedEvent) Apply(account *Account) {
	account.applyMoneyDeposited(e)
}

type MoneyWithdrawnEvent struct {
	AmountWithdrawn int64 `json:"amountWithdrawn"`
	Balance         int64 `json:"balance"`
}

func (e MoneyWithdrawnEvent) Apply(account *Account) {
	account.applyMoneyWithdrawn(e)
}

type AccountClosedEvent struct {
}

func (e AccountClosedEvent) Apply(account *Account) {
	account.applyAccountClosed(e)
}

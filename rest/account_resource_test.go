package rest_test

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/rest"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

type accountResourceFixture struct {
	assert.Assertions
	server *rest.Server
}

func newFixture(t *testing.T) accountResourceFixture {
	return accountResourceFixture{
		Assertions: *assert.New(t),
		server:     rest.NewRestServer(eventstore.NewInMemoryStore(), 0),
	}
}

func (f accountResourceFixture) post(path string) *httptest.ResponseRecorder {
	req, err := http.NewRequest(http.MethodPost, path, nil)
	f.NoError(err)
	recorder := httptest.NewRecorder()

	f.server.ServeHTTP(recorder, req)

	return recorder
}

func (f accountResourceFixture) get(path string) *httptest.ResponseRecorder {
	req, err := http.NewRequest(http.MethodGet, path, nil)
	f.NoError(err)
	recorder := httptest.NewRecorder()

	f.server.ServeHTTP(recorder, req)

	return recorder
}

func (f accountResourceFixture) put(path string) *httptest.ResponseRecorder {
	req, err := http.NewRequest(http.MethodPut, path, nil)
	f.NoError(err)
	recorder := httptest.NewRecorder()

	f.server.ServeHTTP(recorder, req)

	return recorder
}

func (f accountResourceFixture) delete(path string) *httptest.ResponseRecorder {
	req, err := http.NewRequest(http.MethodDelete, path, nil)
	f.NoError(err)
	recorder := httptest.NewRecorder()

	f.server.ServeHTTP(recorder, req)

	return recorder
}

func (f accountResourceFixture) createAccount(accountId account.Id, ownerId account.OwnerId) string {
	req, err := http.NewRequest("POST", "/account/"+accountId.String()+"?owner="+ownerId.String(), nil)
	f.NoError(err)
	recorder := httptest.NewRecorder()

	f.server.ServeHTTP(recorder, req)

	f.Equal(http.StatusCreated, recorder.Code)
	f.Equal("", recorder.Body.String())
	return recorder.Header().Get("Location")
}

func (f accountResourceFixture) queryAccount(accountId account.Id) account.Snapshot {
	req, err := http.NewRequest("GET", "/account/"+accountId.String(), nil)
	f.NoError(err)
	recorder := httptest.NewRecorder()

	f.server.ServeHTTP(recorder, req)

	f.Equal("application/json", recorder.Header().Get("Content-Type"))
	f.Equal(http.StatusOK, recorder.Code)

	var snapshot account.Snapshot
	err = json.Unmarshal(recorder.Body.Bytes(), &snapshot)
	f.NoError(err)
	return snapshot
}

func (f accountResourceFixture) deposit(accountId account.Id, amount int64, txId uuid.UUID) {
	res := f.put("/account/" + accountId.String() + "/deposit?amount=" + strconv.FormatInt(amount, 10) + "&transactionId=" + txId.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func (f accountResourceFixture) withdraw(accountId account.Id, amount int64, txId uuid.UUID) {
	res := f.put("/account/" + accountId.String() + "/withdraw?amount=" + strconv.FormatInt(amount, 10) + "&transactionId=" + txId.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func (f accountResourceFixture) transfer(sourceAccountId account.Id, targetAccountId account.Id, amount int64, txId uuid.UUID) {
	res := f.put("/account/" + sourceAccountId.String() + "/transfer?targetAccount=" + targetAccountId.String() + "&amount=" + strconv.FormatInt(amount, 10) + "&transactionId=" + txId.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func (f accountResourceFixture) close(id account.Id) {
	res := f.delete("/account/" + id.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func TestOpenAccount(t *testing.T) {
	f := newFixture(t)
	accountId, ownerId := account.NewId(), account.NewOwnerId()
	f.createAccount(accountId, ownerId)
}

func TestRequireValidUUIDForAccountId(t *testing.T) {
	f := newFixture(t)

	res := f.post("/account/foobar?owner=" + account.NewOwnerId().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, `{"message":"Invalid UUID string: foobar"}`, res.Body.String())
}

func TestRequireValidUUIDForOwnerId(t *testing.T) {
	f := newFixture(t)

	res := f.post("/account/" + account.NewId().String() + "?owner=foobar")

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, `{"message":"Invalid UUID string: foobar"}`, res.Body.String())
}

func TestConflictOnAccountOpeningWhenAccountAlreadyExists(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	res := f.post("/account/" + accountId.String() + "?owner=" + uuid.New().String())

	assert.Equal(t, http.StatusConflict, res.Code)
	assert.Equal(t, `{"message":"account already exists"}`, res.Body.String())
}

func TestQueryAccount(t *testing.T) {
	f := newFixture(t)
	accountId, ownerId := account.NewId(), account.NewOwnerId()
	resource := f.createAccount(accountId, ownerId)

	res := f.get(resource)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t,
		fmt.Sprintf(
			`{"Id":"%s","OwnerId":"%s","Balance":0,"Open":true}`,
			accountId.String(), ownerId.String()),
		res.Body.String(),
	)
}

func Test404WhenQueryingNonExistentAccount(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()

	res := f.get("/account/" + accountId.String())

	assert.Equal(t, http.StatusNotFound, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"account not found"}`, res.Body.String())
}

func TestDepositMoney(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	f.deposit(accountId, 42, uuid.New())

	snapshot := f.queryAccount(accountId)
	assert.Equal(t, int64(42), snapshot.Balance)
}

func TestDepositsAccumulateBalance(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	f.deposit(accountId, 42, uuid.New())
	f.deposit(accountId, 42, uuid.New())

	snapshot := f.queryAccount(accountId)
	assert.Equal(t, int64(84), snapshot.Balance)
}

func TestDepositsAreIdempotent(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	txId := uuid.New()
	f.deposit(accountId, 42, txId)
	f.deposit(accountId, 42, txId)

	snapshot := f.queryAccount(accountId)
	assert.Equal(t, int64(42), snapshot.Balance)
}

func TestDoNotAcceptFloatingPointDeposit(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	res := f.put("/account/" + accountId.String() + "/deposit?amount=42.4&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got '42.4'"}`, res.Body.String())
}

func TestDoNotAcceptNonNumericDeposit(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	res := f.put("/account/" + accountId.String() + "/deposit?amount=banana&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got 'banana'"}`, res.Body.String())
}

func TestDoNotAcceptNegativeDeposit(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	res := f.put("/account/" + accountId.String() + "/deposit?amount=-1&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"can not deposit negative amount"}`, res.Body.String())
}

func TestWithdrawMoney(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())
	f.deposit(accountId, 42, uuid.New())

	f.withdraw(accountId, 11, uuid.New())

	snapshot := f.queryAccount(accountId)
	assert.Equal(t, int64(31), snapshot.Balance)
}

func TestIdempotentWithdrawals(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())
	f.deposit(accountId, 42, uuid.New())

	txId := uuid.New()
	f.withdraw(accountId, 30, txId)
	f.withdraw(accountId, 30, txId)

	snapshot := f.queryAccount(accountId)
	assert.Equal(t, int64(12), snapshot.Balance)
}

func TestCanNotWithdrawWithInsufficientBalance(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())
	f.deposit(accountId, 42, uuid.New())

	res := f.put("/account/" + accountId.String() + "/withdraw?amount=43&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"insufficient balance"}`, res.Body.String())
}

func TestDoNotAcceptFloatingPointWithdrawal(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())
	f.deposit(accountId, 42, uuid.New())

	res := f.put("/account/" + accountId.String() + "/withdraw?amount=42.2&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got '42.2'"}`, res.Body.String())
}

func TestDoNotAcceptNonNumericWithdrawal(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())
	f.deposit(accountId, 42, uuid.New())

	res := f.put("/account/" + accountId.String() + "/withdraw?amount=banana&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got 'banana'"}`, res.Body.String())
}

func TestDoNotAcceptNegativeWithdrawal(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())
	f.deposit(accountId, 42, uuid.New())

	res := f.put("/account/" + accountId.String() + "/withdraw?amount=-1&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"can not withdraw negative amount"}`, res.Body.String())
}

func TestTransferMoneyBetweenAccounts(t *testing.T) {
	f := newFixture(t)
	sourceAccountId := account.NewId()
	f.createAccount(sourceAccountId, account.NewOwnerId())
	f.deposit(sourceAccountId, 6, uuid.New())

	targetAccountId := account.NewId()
	f.createAccount(targetAccountId, account.NewOwnerId())
	f.deposit(targetAccountId, 1, uuid.New())

	f.transfer(sourceAccountId, targetAccountId, 2, uuid.New())

	sourceSnapshot := f.queryAccount(sourceAccountId)
	assert.Equal(t, int64(4), sourceSnapshot.Balance)

	targetSnapshot := f.queryAccount(targetAccountId)
	assert.Equal(t, int64(3), targetSnapshot.Balance)
}

func TestIdempotentMoneyTransfer(t *testing.T) {
	f := newFixture(t)
	sourceAccountId := account.NewId()
	f.createAccount(sourceAccountId, account.NewOwnerId())
	f.deposit(sourceAccountId, 100, uuid.New())

	targetAccountId := account.NewId()
	f.createAccount(targetAccountId, account.NewOwnerId())

	txId := uuid.New()
	f.transfer(sourceAccountId, targetAccountId, 60, txId)
	f.transfer(sourceAccountId, targetAccountId, 60, txId)

	sourceSnapshot := f.queryAccount(sourceAccountId)
	assert.Equal(t, int64(40), sourceSnapshot.Balance)

	targetSnapshot := f.queryAccount(targetAccountId)
	assert.Equal(t, int64(60), targetSnapshot.Balance)
}

func Test404WhenTransferringToNonExistentAccount(t *testing.T) {
	f := newFixture(t)
	sourceAccountId := account.NewId()
	f.createAccount(sourceAccountId, account.NewOwnerId())
	f.deposit(sourceAccountId, 6, uuid.New())

	txId := uuid.New()
	res := f.put("/account/" + sourceAccountId.String() + "/transfer?targetAccount=" + account.NewId().String() + "&amount=2&transactionId=" + txId.String())
	assert.Equal(t, http.StatusNotFound, res.Code)

	sourceSnapshot := f.queryAccount(sourceAccountId)
	assert.Equal(t, int64(6), sourceSnapshot.Balance)
}

func TestCloseAccount(t *testing.T) {
	f := newFixture(t)
	accountId := account.NewId()
	f.createAccount(accountId, account.NewOwnerId())

	f.close(accountId)

	snapshot := f.queryAccount(accountId)
	assert.False(t, snapshot.Open)
}

func Test404WhenClosingNonExistentAccount(t *testing.T) {
	f := newFixture(t)

	res := f.delete("/account/" + account.NewId().String())

	assert.Equal(t, http.StatusNotFound, res.Code)
}

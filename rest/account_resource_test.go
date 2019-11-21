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

func (f accountResourceFixture) createAccount(accountID account.ID, ownerID account.OwnerID) string {
	req, err := http.NewRequest("POST", "/api/account/"+accountID.String()+"?owner="+ownerID.String(), nil)
	f.NoError(err)
	recorder := httptest.NewRecorder()

	f.server.ServeHTTP(recorder, req)

	f.Equal(http.StatusCreated, recorder.Code)
	f.Equal("", recorder.Body.String())
	return recorder.Header().Get("Location")
}

func (f accountResourceFixture) queryAccount(accountID account.ID) account.Snapshot {
	req, err := http.NewRequest("GET", "/api/account/"+accountID.String(), nil)
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

func (f accountResourceFixture) deposit(accountID account.ID, amount int64, txId uuid.UUID) {
	res := f.put("/api/account/" + accountID.String() + "/deposit?amount=" + strconv.FormatInt(amount, 10) + "&transactionId=" + txId.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func (f accountResourceFixture) withdraw(accountID account.ID, amount int64, txId uuid.UUID) {
	res := f.put("/api/account/" + accountID.String() + "/withdraw?amount=" + strconv.FormatInt(amount, 10) + "&transactionId=" + txId.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func (f accountResourceFixture) transfer(sourceaccountID account.ID, targetaccountID account.ID, amount int64, txId uuid.UUID) {
	res := f.put("/api/account/" + sourceaccountID.String() + "/transfer?targetAccount=" + targetaccountID.String() + "&amount=" + strconv.FormatInt(amount, 10) + "&transactionId=" + txId.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func (f accountResourceFixture) close(id account.ID) {
	res := f.delete("/api/account/" + id.String())
	f.Equal(http.StatusNoContent, res.Code)
}

func TestOpenAccount(t *testing.T) {
	f := newFixture(t)
	accountID, ownerID := account.NewID(), account.NewOwnerID()
	location := f.createAccount(accountID, ownerID)
	assert.Equal(t, "/api/account/"+accountID.String(), location)
}

func TestRequireValidUUIDForaccountID(t *testing.T) {
	f := newFixture(t)

	res := f.post("/api/account/foobar?owner=" + account.NewOwnerID().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, `{"message":"Invalid UUID string: foobar"}`, res.Body.String())
}

func TestRequireValidUUIDForownerID(t *testing.T) {
	f := newFixture(t)

	res := f.post("/api/account/" + account.NewID().String() + "?owner=foobar")

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, `{"message":"Invalid UUID string: foobar"}`, res.Body.String())
}

func TestConflictOnAccountOpeningWhenAccountAlreadyExists(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	res := f.post("/api/account/" + accountID.String() + "?owner=" + uuid.New().String())

	assert.Equal(t, http.StatusConflict, res.Code)
	assert.Equal(t, `{"message":"account already exists"}`, res.Body.String())
}

func TestQueryAccount(t *testing.T) {
	f := newFixture(t)
	accountID, ownerID := account.NewID(), account.NewOwnerID()
	resource := f.createAccount(accountID, ownerID)

	res := f.get(resource)

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t,
		fmt.Sprintf(
			`{"ID":"%s","OwnerID":"%s","Balance":0,"Open":true}`,
			accountID.String(), ownerID.String()),
		res.Body.String(),
	)
}

func Test404WhenQueryingNonExistentAccount(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()

	res := f.get("/api/account/" + accountID.String())

	assert.Equal(t, http.StatusNotFound, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"account not found"}`, res.Body.String())
}

func TestDepositMoney(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	f.deposit(accountID, 42, uuid.New())

	snapshot := f.queryAccount(accountID)
	assert.Equal(t, int64(42), snapshot.Balance)
}

func TestDepositsAccumulateBalance(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	f.deposit(accountID, 42, uuid.New())
	f.deposit(accountID, 42, uuid.New())

	snapshot := f.queryAccount(accountID)
	assert.Equal(t, int64(84), snapshot.Balance)
}

func TestDepositsAreIdempotent(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	txId := uuid.New()
	f.deposit(accountID, 42, txId)
	f.deposit(accountID, 42, txId)

	snapshot := f.queryAccount(accountID)
	assert.Equal(t, int64(42), snapshot.Balance)
}

func TestDoNotAcceptFloatingPointDeposit(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	res := f.put("/api/account/" + accountID.String() + "/deposit?amount=42.4&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got '42.4'"}`, res.Body.String())
}

func TestDoNotAcceptNonNumericDeposit(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	res := f.put("/api/account/" + accountID.String() + "/deposit?amount=banana&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got 'banana'"}`, res.Body.String())
}

func TestDoNotAcceptNegativeDeposit(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	res := f.put("/api/account/" + accountID.String() + "/deposit?amount=-1&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"can not deposit negative amount"}`, res.Body.String())
}

func TestWithdrawMoney(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())
	f.deposit(accountID, 42, uuid.New())

	f.withdraw(accountID, 11, uuid.New())

	snapshot := f.queryAccount(accountID)
	assert.Equal(t, int64(31), snapshot.Balance)
}

func TestIdempotentWithdrawals(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())
	f.deposit(accountID, 42, uuid.New())

	txId := uuid.New()
	f.withdraw(accountID, 30, txId)
	f.withdraw(accountID, 30, txId)

	snapshot := f.queryAccount(accountID)
	assert.Equal(t, int64(12), snapshot.Balance)
}

func TestCanNotWithdrawWithInsufficientBalance(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())
	f.deposit(accountID, 42, uuid.New())

	res := f.put("/api/account/" + accountID.String() + "/withdraw?amount=43&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"insufficient balance"}`, res.Body.String())
}

func TestDoNotAcceptFloatingPointWithdrawal(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())
	f.deposit(accountID, 42, uuid.New())

	res := f.put("/api/account/" + accountID.String() + "/withdraw?amount=42.2&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got '42.2'"}`, res.Body.String())
}

func TestDoNotAcceptNonNumericWithdrawal(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())
	f.deposit(accountID, 42, uuid.New())

	res := f.put("/api/account/" + accountID.String() + "/withdraw?amount=banana&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got 'banana'"}`, res.Body.String())
}

func TestDoNotAcceptNegativeWithdrawal(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())
	f.deposit(accountID, 42, uuid.New())

	res := f.put("/api/account/" + accountID.String() + "/withdraw?amount=-1&transactionId=" + uuid.New().String())

	assert.Equal(t, http.StatusBadRequest, res.Code)
	assert.Equal(t, "application/json", res.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"can not withdraw negative amount"}`, res.Body.String())
}

func TestTransferMoneyBetweenAccounts(t *testing.T) {
	f := newFixture(t)
	sourceaccountID := account.NewID()
	f.createAccount(sourceaccountID, account.NewOwnerID())
	f.deposit(sourceaccountID, 6, uuid.New())

	targetaccountID := account.NewID()
	f.createAccount(targetaccountID, account.NewOwnerID())
	f.deposit(targetaccountID, 1, uuid.New())

	f.transfer(sourceaccountID, targetaccountID, 2, uuid.New())

	sourceSnapshot := f.queryAccount(sourceaccountID)
	assert.Equal(t, int64(4), sourceSnapshot.Balance)

	targetSnapshot := f.queryAccount(targetaccountID)
	assert.Equal(t, int64(3), targetSnapshot.Balance)
}

func TestIdempotentMoneyTransfer(t *testing.T) {
	f := newFixture(t)
	sourceaccountID := account.NewID()
	f.createAccount(sourceaccountID, account.NewOwnerID())
	f.deposit(sourceaccountID, 100, uuid.New())

	targetaccountID := account.NewID()
	f.createAccount(targetaccountID, account.NewOwnerID())

	txId := uuid.New()
	f.transfer(sourceaccountID, targetaccountID, 60, txId)
	f.transfer(sourceaccountID, targetaccountID, 60, txId)

	sourceSnapshot := f.queryAccount(sourceaccountID)
	assert.Equal(t, int64(40), sourceSnapshot.Balance)

	targetSnapshot := f.queryAccount(targetaccountID)
	assert.Equal(t, int64(60), targetSnapshot.Balance)
}

func Test404WhenTransferringToNonExistentAccount(t *testing.T) {
	f := newFixture(t)
	sourceaccountID := account.NewID()
	f.createAccount(sourceaccountID, account.NewOwnerID())
	f.deposit(sourceaccountID, 6, uuid.New())

	txId := uuid.New()
	res := f.put("/api/account/" + sourceaccountID.String() + "/transfer?targetAccount=" + account.NewID().String() + "&amount=2&transactionId=" + txId.String())
	assert.Equal(t, http.StatusNotFound, res.Code)

	sourceSnapshot := f.queryAccount(sourceaccountID)
	assert.Equal(t, int64(6), sourceSnapshot.Balance)
}

func TestCloseAccount(t *testing.T) {
	f := newFixture(t)
	accountID := account.NewID()
	f.createAccount(accountID, account.NewOwnerID())

	f.close(accountID)

	snapshot := f.queryAccount(accountID)
	assert.False(t, snapshot.Open)
}

func Test404WhenClosingNonExistentAccount(t *testing.T) {
	f := newFixture(t)

	res := f.delete("/api/account/" + account.NewID().String())

	assert.Equal(t, http.StatusNotFound, res.Code)
}

func TestQueryAccountEvents(t *testing.T) {
	f := newFixture(t)
	accountID, ownerID := account.NewID(), account.NewOwnerID()
	f.createAccount(accountID, ownerID)
	f.deposit(accountID, 5, uuid.New())
	f.deposit(accountID, 12, uuid.New())

	res := f.get("/api/account/" + accountID.String() + "/events")

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(
		t,
		fmt.Sprintf(
			`[{"AggregateId":"%s","Seq":1,"Event":{"AccountID":"%s","OwnerID":"%s"}},{"AggregateId":"%s","Seq":2,"Event":{"AmountDeposited":5,"Balance":5}},{"AggregateId":"%s","Seq":3,"Event":{"AmountDeposited":12,"Balance":17}}]`,
			accountID, accountID, ownerID, accountID, accountID,
		),
		res.Body.String(),
	)
}

func TestQueryNonExistingAccountNoEvents(t *testing.T) {
	f := newFixture(t)

	res := f.get("/api/account/" + account.NewID().String() + "/events")

	assert.Equal(t, http.StatusOK, res.Code)
	assert.Equal(t, `[]`, res.Body.String())
}

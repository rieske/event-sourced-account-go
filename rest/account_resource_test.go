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
	"testing"
)

var server = rest.NewRestServer(eventstore.NewInMemoryStore(), 0)

func createAccount(t *testing.T, accountId account.Id, ownerId account.OwnerId) string {
	req, err := http.NewRequest("POST", "/account/"+accountId.String()+"?owner="+ownerId.String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusCreated, recorder.Code)
	assert.Equal(t, "", recorder.Body.String())
	return recorder.Header().Get("Location")
}

func queryAccount(t *testing.T, accountId account.Id) account.Snapshot {
	req, err := http.NewRequest("GET", "/account/"+accountId.String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	assert.Equal(t, http.StatusOK, recorder.Code)

	var snapshot account.Snapshot
	err = json.Unmarshal(recorder.Body.Bytes(), &snapshot)
	assert.NoError(t, err)
	return snapshot
}

func TestOpenAccount(t *testing.T) {
	accountId, ownerId := account.NewId(), account.NewOwnerId()
	createAccount(t, accountId, ownerId)
}

func TestRequireValidUUIDForAccountId(t *testing.T) {
	ownerId := account.NewOwnerId()
	req, err := http.NewRequest("POST", "/account/foobar?owner="+ownerId.String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Equal(t, `{"message":"Invalid UUID string: foobar"}`, recorder.Body.String())
}

func TestRequireValidUUIDForOwnerId(t *testing.T) {
	accountId := account.NewId()
	req, err := http.NewRequest("POST", "/account/"+accountId.String()+"?owner=foobar", nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Equal(t, `{"message":"Invalid UUID string: foobar"}`, recorder.Body.String())
}

func TestConflictOnAccountOpeningWhenAccountAlreadyExists(t *testing.T) {
	accountId := account.NewId()
	createAccount(t, accountId, account.NewOwnerId())

	req, err := http.NewRequest("POST", "/account/"+accountId.String()+"?owner="+uuid.New().String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusConflict, recorder.Code)
	assert.Equal(t, `{"message":"account already exists"}`, recorder.Body.String())
}

func TestQueryAccount(t *testing.T) {
	accountId, ownerId := account.NewId(), account.NewOwnerId()
	resource := createAccount(t, accountId, ownerId)

	req, err := http.NewRequest("GET", resource, nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	assert.Equal(t,
		fmt.Sprintf(
			`{"Id":"%s","OwnerId":"%s","Balance":0,"Open":true}`,
			accountId.String(), ownerId.String()),
		recorder.Body.String(),
	)
}

func Test404WhenQueryingNonExistentAccount(t *testing.T) {
	accountId := account.NewId()

	req, err := http.NewRequest("GET", "/account/"+accountId.String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusNotFound, recorder.Code)
	assert.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"account not found"}`, recorder.Body.String())
}

func TestDepositMoney(t *testing.T) {
	accountId := account.NewId()
	createAccount(t, accountId, account.NewOwnerId())

	req, err := http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=42&transactionId="+uuid.New().String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusNoContent, recorder.Code)

	snapshot := queryAccount(t, accountId)
	assert.Equal(t, int64(42), snapshot.Balance)
}

func TestDepositsAccumulateBalance(t *testing.T) {
	accountId := account.NewId()
	createAccount(t, accountId, account.NewOwnerId())

	req, err := http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=42&transactionId="+uuid.New().String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusNoContent, recorder.Code)

	req, err = http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=42&transactionId="+uuid.New().String(), nil)
	assert.NoError(t, err)
	server.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusNoContent, recorder.Code)

	snapshot := queryAccount(t, accountId)
	assert.Equal(t, int64(84), snapshot.Balance)
}

func TestDepositsAreIdempotent(t *testing.T) {
	accountId := account.NewId()
	createAccount(t, accountId, account.NewOwnerId())

	txId := uuid.New()

	req, err := http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=42&transactionId="+txId.String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusNoContent, recorder.Code)

	req, err = http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=42&transactionId="+txId.String(), nil)
	assert.NoError(t, err)
	server.ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusNoContent, recorder.Code)

	snapshot := queryAccount(t, accountId)
	assert.Equal(t, int64(42), snapshot.Balance)
}

func TestDoNotAcceptFloatingPointDeposit(t *testing.T) {
	accountId := account.NewId()
	createAccount(t, accountId, account.NewOwnerId())

	req, err := http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=42.4&transactionId="+uuid.New().String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got '42.4'"}`, recorder.Body.String())
}

func TestDoNotAcceptNonNumericDeposit(t *testing.T) {
	accountId := account.NewId()
	createAccount(t, accountId, account.NewOwnerId())

	req, err := http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=banana&transactionId="+uuid.New().String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"integer amount required, got 'banana'"}`, recorder.Body.String())
}

func TestDoNotAcceptNegativeDeposit(t *testing.T) {
	accountId := account.NewId()
	createAccount(t, accountId, account.NewOwnerId())

	req, err := http.NewRequest("PUT", "/account/"+accountId.String()+"/deposit?amount=-1&transactionId="+uuid.New().String(), nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	assert.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
	assert.Equal(t, `{"message":"can not deposit negative amount"}`, recorder.Body.String())
}

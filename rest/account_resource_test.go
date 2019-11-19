package rest_test

import (
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

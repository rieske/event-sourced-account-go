package rest_test

import (
	"github.com/rieske/event-sourced-account-go/eventstore"
	"github.com/rieske/event-sourced-account-go/rest"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPing(t *testing.T) {
	server := rest.NewRestServer(eventstore.NewInMemoryStore(), 0)

	req, err := http.NewRequest(http.MethodGet, "/ping", nil)
	assert.NoError(t, err)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "pong", recorder.Body.String())
}

package serialization

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
	"testing"
)

type accountOpenedJsonTestFixture struct {
	event           account.AccountOpenedEvent
	serializedEvent []byte
}

func newAccountOpenedJsonTestFixture(t *testing.T) accountOpenedJsonTestFixture {
	accountId, err := uuid.Parse("ce7d9c87-e348-406b-933b-0c6dfc0f014e")
	assert.NoError(t, err)
	ownerId, err := uuid.Parse("c2b0bbce-679a-4af5-9a75-8958da9eb02c")
	assert.NoError(t, err)

	return accountOpenedJsonTestFixture{
		event: account.AccountOpenedEvent{
			AccountId: account.Id{accountId},
			OwnerId:   account.OwnerId{ownerId},
		},
		serializedEvent: []byte(`{"AccountId":"ce7d9c87-e348-406b-933b-0c6dfc0f014e","OwnerId":"c2b0bbce-679a-4af5-9a75-8958da9eb02c"}`),
	}
}

func TestJsonSerializeAccountOpened(t *testing.T) {
	fixture := newAccountOpenedJsonTestFixture(t)

	serializedEvent, err := json.Marshal(fixture.event)

	assert.NoError(t, err)
	assert.Equal(t, fixture.serializedEvent, serializedEvent)
}

func TestJsonDeserializeAccountOpened(t *testing.T) {
	fixture := newAccountOpenedJsonTestFixture(t)

	var event account.AccountOpenedEvent
	err := json.Unmarshal(fixture.serializedEvent, &event)

	// then
	assert.NoError(t, err)
	assert.Equal(t, fixture.event, event)
}

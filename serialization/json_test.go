package serialization

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonSerializeAccountOpened(t *testing.T) {
	// given
	accountId, err := uuid.Parse("ce7d9c87-e348-406b-933b-0c6dfc0f014e")
	assert.NoError(t, err)
	ownerId, err := uuid.Parse("c2b0bbce-679a-4af5-9a75-8958da9eb02c")
	assert.NoError(t, err)

	event := account.AccountOpenedEvent{
		AccountId: account.Id{accountId},
		OwnerId:   account.OwnerId{ownerId},
	}

	// when
	serialized, err := json.Marshal(event)

	// then
	assert.NoError(t, err)
	assert.Equal(
		t,
		`{"AccountId":"ce7d9c87-e348-406b-933b-0c6dfc0f014e","OwnerId":"c2b0bbce-679a-4af5-9a75-8958da9eb02c"}`,
		string(serialized[:]),
	)
}

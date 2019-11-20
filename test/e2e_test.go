// +build e2e

package test

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

const (
	serviceUrl     = "http://localhost:8080"
	composeCommand = "docker-compose"
	composeFile    = "../docker-compose.yml"
)

func waitForPing() {
	for i := 0; i < 30; i++ {
		res, err := http.Get(serviceUrl + "/ping")
		if err == nil && res.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(time.Second * 1)
	}
	log.Println("Service is healthy")
}

func composeUp() {
	cmd := exec.Command(composeCommand, "-f", composeFile, "up", "--build", "-d")
	log.Println(cmd)
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func composeDown() {
	cmd := exec.Command(composeCommand, "-f", composeFile, "down", "--volumes")
	log.Println(cmd)
	if err := cmd.Run(); err != nil {
		panic(err)
	}
}

func TestMain(m *testing.M) {
	composeUp()
	waitForPing()

	code := m.Run()
	composeDown()

	os.Exit(code)
}

func TestPing(t *testing.T) {
	res, err := http.Get(serviceUrl + "/ping")

	assert.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(t, "pong", string(body))
}

func TestCreateAndQueryAccount(t *testing.T) {
	accountID, ownerID := uuid.New().String(), uuid.New().String()

	res, err := http.Post(serviceUrl+"/account/"+accountID+"?owner="+ownerID, "", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, res.StatusCode)

	accountLocation, err := res.Location()
	require.NoError(t, err)

	res, err = http.Get(accountLocation.String())
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := ioutil.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Equal(
		t,
		fmt.Sprintf(`{"ID":"%s","OwnerID":"%s","Balance":0,"Open":true}`, accountID, ownerID),
		string(body),
	)
}

// +build e2e

package test

import (
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, http.StatusOK, res.StatusCode)
	body, err := ioutil.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Equal(t, "pong", string(body))
}

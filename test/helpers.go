package test

import "testing"

func ExpectError(t *testing.T, err error, message string) {
	if err == nil {
		t.Errorf("error expected - %s", message)
		return
	}
	if err.Error() != message {
		t.Errorf("error expected - %s, got %s", message, err.Error())
	}
}

func ExpectNoError(t *testing.T, err error) {
	if err != nil {
		t.Error("no error expected, got:", err)
	}
}

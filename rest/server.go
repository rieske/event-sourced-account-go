package rest

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"log"
	"net/http"
	"path"
	"strconv"
	"strings"
)

type Server struct {
	accountResource accountResource
}

type response struct {
	status  int
	body    []byte
	headers map[string]string
}

const (
	contentTypeHeader = "Content-Type"
	locationHeader    = "Location"
)

func responseWithBody(status int, contentType string, body []byte) response {
	return response{
		status:  status,
		body:    body,
		headers: map[string]string{contentTypeHeader: contentType},
	}
}

func jsonResponse(status int, body []byte) response {
	return response{
		status:  status,
		body:    body,
		headers: map[string]string{contentTypeHeader: "application/json"},
	}
}

func locationResponse(status int, location string) response {
	return response{
		status:  status,
		headers: map[string]string{locationHeader: location},
	}
}

func notFoundResponse() response {
	return response{status: http.StatusNotFound}
}

func noContentResponse() response {
	return response{status: http.StatusNoContent}
}

func conflictResponse() response {
	return response{status: http.StatusConflict}
}

func errorResponse(statusCode int, err error) response {
	return jsonResponse(statusCode, []byte(fmt.Sprintf(`{"message":"%s"}`, err.Error())))
}

func unhandledErrorResponse(err error) response {
	log.Println(err)
	return errorResponse(http.StatusInternalServerError, err)
}

func NewRestServer(store eventsourcing.EventStore, snapshottingFrequency int) *Server {
	return &Server{
		accountResource: accountResource{
			accountService: eventsourcing.NewAccountService(store, snapshottingFrequency),
		},
	}
}

func (s *Server) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	correlationId := uuid.New().String()
	log.Printf("[%v] %v %v", correlationId, req.Method, req.URL.Path)

	var head string
	r := notFoundResponse()
	head, req.URL.Path = shiftPath(req.URL.Path)
	switch head {
	case "api":
		head, req.URL.Path = shiftPath(req.URL.Path)
		switch head {
		case "account":
			r = s.accountResource.handle(res, req)
		}
	case "ping":
		r = responseWithBody(http.StatusOK, "text/plain", []byte("pong"))
	}

	for n, h := range r.headers {
		res.Header().Set(n, h)
	}
	res.WriteHeader(r.status)
	writeBody(res, r.body)

	log.Printf("[%v] %v", correlationId, r.status)
}

// shiftPath splits off the first component of p, which will be cleaned of
// relative components before processing. head will never contain a slash and
// tail will always be a rooted path without trailing slash.
func shiftPath(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}

func writeBody(res http.ResponseWriter, body []byte) {
	if _, err := res.Write(body); err != nil {
		log.Println(err)
		http.Error(res, "Could not write response", http.StatusInternalServerError)
	}
}

func parseUUID(uuidStr string) (uuid.UUID, *response) {
	id, err := uuid.Parse(uuidStr)
	if err != nil {
		r := jsonResponse(http.StatusBadRequest, []byte(fmt.Sprintf(`{"message":"Invalid UUID string: %s"}`, uuidStr)))
		return id, &r
	}
	return id, nil
}

func parseAmount(amountStr string) (int64, *response) {
	amount, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		r := jsonResponse(http.StatusBadRequest, []byte(fmt.Sprintf(`{"message":"integer amount required, got '%s'"}`, amountStr)))
		return amount, &r
	}
	return amount, nil
}

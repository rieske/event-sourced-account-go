package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

type Server struct {
	accountResource accountResource
}

type accountResource struct {
	accountService *eventsourcing.AccountService
}

func NewRestServer(store eventsourcing.EventStore, snapshottingFrequency int) *Server {
	return &Server{
		accountResource: accountResource{
			accountService: eventsourcing.NewAccountService(store, snapshottingFrequency),
		},
	}
}

func (s *Server) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	var head string
	head, req.URL.Path = shiftPath(req.URL.Path)
	if head == "account" {
		s.accountResource.ServeHTTP(res, req)
		return
	}

	http.Error(res, "Not Found", http.StatusNotFound)
}

func (r *accountResource) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	var head string
	head, req.URL.Path = shiftPath(req.URL.Path)

	accountId, ok := parseUUID(res, head)
	if !ok {
		return
	}

	switch req.Method {
	case "POST":
		r.post(res, account.Id{accountId}, req.URL.Query())
	case "GET":
		r.get(res, account.Id{accountId})
	case "PUT":
		r.put(res, account.Id{accountId}, req.URL.Query())
	default:
		respondWithError(res, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	}
}

func (r *accountResource) post(res http.ResponseWriter, accountId account.Id, query url.Values) {
	ownerId, ok := parseUUID(res, query.Get("owner"))
	if !ok {
		return
	}
	switch err := r.accountService.OpenAccount(accountId, account.OwnerId{ownerId}); err {
	case nil:
		break
	case account.Exists:
		respondWithError(res, http.StatusConflict, err)
		return
	default:
		log.Println(err)
		respondWithError(res, http.StatusInternalServerError, err)
		return
	}

	res.Header().Set("Location", "/account/"+accountId.String())
	res.WriteHeader(http.StatusCreated)
}

func (r *accountResource) get(res http.ResponseWriter, id account.Id) {
	snapshot, err := r.accountService.QueryAccount(id)
	switch err {
	case nil:
		break
	case account.NotFound:
		respondWithError(res, http.StatusNotFound, err)
		return
	default:
		log.Println(err)
		respondWithError(res, http.StatusInternalServerError, err)
		return
	}

	response, err := json.Marshal(snapshot)
	if err != nil {
		log.Println(err)
		respondWithError(res, http.StatusInternalServerError, err)
		return
	}
	respondWithJson(res, response)
}

func (r *accountResource) put(res http.ResponseWriter, id account.Id, query url.Values) {
	amount, err := strconv.ParseInt(query.Get("amount"), 10, 64)
	if err != nil {
		respondWithError(res, http.StatusBadRequest, fmt.Errorf("integer amount required, got '%s'", query.Get("amount")))
		return
	}
	txId, ok := parseUUID(res, query.Get("transactionId"))
	if !ok {
		return
	}
	switch err := r.accountService.Deposit(id, txId, amount); err {
	case nil:
		break
	case account.NegativeDeposit:
		respondWithError(res, http.StatusBadRequest, err)
		return
	}

	res.WriteHeader(http.StatusNoContent)
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

func respondWithJson(res http.ResponseWriter, json []byte) {
	res.Header().Set("Content-Type", "application/json")
	if _, err := res.Write(json); err != nil {
		log.Println(err)
		http.Error(res, "Could not write response", http.StatusInternalServerError)
	}
}

func parseUUID(res http.ResponseWriter, uuidStr string) (uuid.UUID, bool) {
	id, err := uuid.Parse(uuidStr)
	if err != nil {
		res.WriteHeader(http.StatusBadRequest)
		respondWithJson(res, []byte(fmt.Sprintf(`{"message":"Invalid UUID string: %s"}`, uuidStr)))
		return id, false
	}
	return id, true
}

func respondWithError(res http.ResponseWriter, statusCode int, err error) {
	res.WriteHeader(statusCode)
	respondWithJson(res, []byte(fmt.Sprintf(`{"message":"%s"}`, err.Error())))
}

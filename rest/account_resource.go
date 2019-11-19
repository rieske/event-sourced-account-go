package rest

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"log"
	"net/http"
	"net/url"
	"path"
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
		r.createAccount(res, account.Id{accountId}, req.URL.Query())
	case "GET":
		r.getAccount(res, account.Id{accountId})
	default:
		http.Error(res, "Only GET and POST are allowed", http.StatusMethodNotAllowed)
	}
}

func (r *accountResource) createAccount(res http.ResponseWriter, accountId account.Id, query url.Values) {
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
		respondWithError(res, http.StatusInternalServerError, err)
		return
	}

	res.Header().Set("Location", "/account/"+accountId.String())
	res.WriteHeader(http.StatusCreated)
}

func (r *accountResource) getAccount(res http.ResponseWriter, id account.Id) {
	snapshot, err := r.accountService.QueryAccount(id)
	if err != nil {
		// TODO: need to distinguish domain and infra errors
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

package rest

import (
	"encoding/json"
	"errors"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"net/http"
	"net/url"
)

type accountResource struct {
	accountService *eventsourcing.AccountService
}

func (r *accountResource) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	var head string
	head, req.URL.Path = shiftPath(req.URL.Path)

	accountID, ok := parseUUID(res, head)
	if !ok {
		return
	}

	switch req.Method {
	case http.MethodPost:
		r.post(res, account.ID{accountID}, req.URL.Query())
	case http.MethodGet:
		head, req.URL.Path = shiftPath(req.URL.Path)
		r.get(res, head, account.ID{accountID})
	case http.MethodPut:
		head, req.URL.Path = shiftPath(req.URL.Path)
		r.put(res, head, account.ID{accountID}, req.URL.Query())
	case http.MethodDelete:
		r.delete(res, account.ID{accountID})
	default:
		respondWithError(res, http.StatusMethodNotAllowed, errors.New("method not allowed"))
	}
}

func (r *accountResource) post(res http.ResponseWriter, accountID account.ID, query url.Values) {
	ownerID, ok := parseUUID(res, query.Get("owner"))
	if !ok {
		return
	}

	if err := r.accountService.OpenAccount(accountID, account.OwnerID{ownerID}); err != nil {
		handleDomainError(res, err)
		return
	}

	res.Header().Set("Location", "/api/account/"+accountID.String())
	res.WriteHeader(http.StatusCreated)
}

func (r *accountResource) get(res http.ResponseWriter, action string, id account.ID) {
	switch action {
	case "":
		r.queryAccount(res, id)
	case "events":
		r.queryEvents(res, id)
	default:
		respondWithError(res, http.StatusBadRequest, errors.New("action not supported"))
	}
}

func (r *accountResource) put(res http.ResponseWriter, action string, id account.ID, query url.Values) {
	switch action {
	case "deposit":
		r.deposit(res, id, query)
	case "withdraw":
		r.withdraw(res, id, query)
	case "transfer":
		r.transfer(res, id, query)
	default:
		respondWithError(res, http.StatusBadRequest, errors.New("action not supported"))
	}
}

func (r *accountResource) queryAccount(res http.ResponseWriter, id account.ID) {
	snapshot, err := r.accountService.QueryAccount(id)
	if err != nil {
		handleDomainError(res, err)
		return
	}

	response, err := json.Marshal(snapshot)
	if err != nil {
		unhandledError(res, err)
		return
	}
	respondWithJson(res, response)
}

func (r *accountResource) queryEvents(res http.ResponseWriter, id account.ID) {
	events, err := r.accountService.Events(id)
	if err != nil {
		handleDomainError(res, err)
		return
	}

	response, err := json.Marshal(events)
	if err != nil {
		unhandledError(res, err)
		return
	}
	respondWithJson(res, response)
}

func (r *accountResource) deposit(res http.ResponseWriter, id account.ID, query url.Values) {
	amount, ok := parseAmount(res, query.Get("amount"))
	if !ok {
		return
	}
	txId, ok := parseUUID(res, query.Get("transactionId"))
	if !ok {
		return
	}

	if err := r.accountService.Deposit(id, txId, amount); err != nil {
		handleDomainError(res, err)
		return
	}

	res.WriteHeader(http.StatusNoContent)
}

func (r *accountResource) withdraw(res http.ResponseWriter, id account.ID, query url.Values) {
	amount, ok := parseAmount(res, query.Get("amount"))
	if !ok {
		return
	}
	txId, ok := parseUUID(res, query.Get("transactionId"))
	if !ok {
		return
	}

	if err := r.accountService.Withdraw(id, txId, amount); err != nil {
		handleDomainError(res, err)
		return
	}

	res.WriteHeader(http.StatusNoContent)
}

func (r *accountResource) delete(res http.ResponseWriter, id account.ID) {
	if err := r.accountService.CloseAccount(id); err != nil {
		handleDomainError(res, err)
		return
	}

	res.WriteHeader(http.StatusNoContent)
}

func (r *accountResource) transfer(res http.ResponseWriter, sourceAccountId account.ID, query url.Values) {
	targetAccountId, ok := parseUUID(res, query.Get("targetAccount"))
	if !ok {
		return
	}
	amount, ok := parseAmount(res, query.Get("amount"))
	if !ok {
		return
	}
	txId, ok := parseUUID(res, query.Get("transactionId"))
	if !ok {
		return
	}

	if err := r.accountService.Transfer(sourceAccountId, account.ID{targetAccountId}, txId, amount); err != nil {
		handleDomainError(res, err)
		return
	}

	res.WriteHeader(http.StatusNoContent)
}

func handleDomainError(res http.ResponseWriter, err error) {
	switch err {
	case account.Exists:
		respondWithError(res, http.StatusConflict, err)
	case account.NotFound:
		respondWithError(res, http.StatusNotFound, err)
	case account.NegativeDeposit:
		respondWithError(res, http.StatusBadRequest, err)
	case account.NegativeWithdrawal:
		respondWithError(res, http.StatusBadRequest, err)
	case account.InsufficientBalance:
		respondWithError(res, http.StatusBadRequest, err)
	case account.ConcurrentModification:
		res.WriteHeader(http.StatusConflict)
	default:
		unhandledError(res, err)
	}
}

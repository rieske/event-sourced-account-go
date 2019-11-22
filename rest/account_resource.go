package rest

import (
	"encoding/json"
	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
	"net/http"
	"net/url"
)

type accountResource struct {
	accountService *eventsourcing.AccountService
}

func (r *accountResource) handle(res http.ResponseWriter, req *http.Request) response {
	var head string
	head, req.URL.Path = shiftPath(req.URL.Path)

	accountID, response := parseUUID(head)
	if response != nil {
		return *response
	}

	switch req.Method {
	case http.MethodPost:
		return r.post(account.ID{accountID}, req.URL.Query())
	case http.MethodGet:
		head, req.URL.Path = shiftPath(req.URL.Path)
		return r.get(head, account.ID{accountID})
	case http.MethodPut:
		head, req.URL.Path = shiftPath(req.URL.Path)
		return r.put(head, account.ID{accountID}, req.URL.Query())
	case http.MethodDelete:
		return r.delete(account.ID{accountID})
	}
	return errorResponse(http.StatusMethodNotAllowed, "method not allowed")
}

func (r *accountResource) post(accountID account.ID, query url.Values) response {
	ownerID, response := parseUUID(query.Get("owner"))
	if response != nil {
		return *response
	}

	if err := r.accountService.OpenAccount(accountID, account.OwnerID{ownerID}); err != nil {
		return handleDomainError(err)
	}

	return locationResponse(http.StatusCreated, "/api/account/"+accountID.String())
}

func (r *accountResource) get(action string, id account.ID) response {
	switch action {
	case "":
		return r.queryAccount(id)
	case "events":
		return r.queryEvents(id)
	default:
		return actionNotSupported()
	}
}

func actionNotSupported() response {
	return jsonResponse(http.StatusBadRequest, []byte(`{"message":"action not supported"}`))
}

func (r *accountResource) put(action string, id account.ID, query url.Values) response {
	switch action {
	case "deposit":
		return r.deposit(id, query)
	case "withdraw":
		return r.withdraw(id, query)
	case "transfer":
		return r.transfer(id, query)
	default:
		return actionNotSupported()
	}
}

func (r *accountResource) queryAccount(id account.ID) response {
	snapshot, err := r.accountService.QueryAccount(id)
	if err != nil {
		return handleDomainError(err)
	}

	response, err := json.Marshal(snapshot)
	if err != nil {
		return unhandledErrorResponse(err)
	}
	return jsonResponse(http.StatusOK, response)
}

func (r *accountResource) queryEvents(id account.ID) response {
	events, err := r.accountService.Events(id)
	if err != nil {
		return handleDomainError(err)
	}

	response, err := json.Marshal(events)
	if err != nil {
		return unhandledErrorResponse(err)
	}
	return jsonResponse(http.StatusOK, response)
}

func (r *accountResource) deposit(id account.ID, query url.Values) response {
	amount, response := parseAmount(query.Get("amount"))
	if response != nil {
		return *response
	}
	txId, response := parseUUID(query.Get("transactionId"))
	if response != nil {
		return *response
	}

	if err := r.accountService.Deposit(id, txId, amount); err != nil {
		return handleDomainError(err)
	}

	return noContentResponse()
}

func (r *accountResource) withdraw(id account.ID, query url.Values) response {
	amount, response := parseAmount(query.Get("amount"))
	if response != nil {
		return *response
	}
	txId, response := parseUUID(query.Get("transactionId"))
	if response != nil {
		return *response
	}

	if err := r.accountService.Withdraw(id, txId, amount); err != nil {
		return handleDomainError(err)
	}

	return noContentResponse()
}

func (r *accountResource) delete(id account.ID) response {
	if err := r.accountService.CloseAccount(id); err != nil {
		return handleDomainError(err)
	}

	return response{status: http.StatusNoContent}
}

func (r *accountResource) transfer(sourceAccountId account.ID, query url.Values) response {
	targetAccountId, response := parseUUID(query.Get("targetAccount"))
	if response != nil {
		return *response
	}
	amount, response := parseAmount(query.Get("amount"))
	if response != nil {
		return *response
	}
	txId, response := parseUUID(query.Get("transactionId"))
	if response != nil {
		return *response
	}

	if err := r.accountService.Transfer(sourceAccountId, account.ID{targetAccountId}, txId, amount); err != nil {
		return handleDomainError(err)
	}

	return noContentResponse()
}

func handleDomainError(err error) response {
	switch err {
	case account.Exists:
		return errorResponse(http.StatusConflict, err.Error())
	case account.NotFound:
		return errorResponse(http.StatusNotFound, err.Error())
	case account.NegativeDeposit:
		return errorResponse(http.StatusBadRequest, err.Error())
	case account.NegativeWithdrawal:
		return errorResponse(http.StatusBadRequest, err.Error())
	case account.InsufficientBalance:
		return errorResponse(http.StatusBadRequest, err.Error())
	case account.ConcurrentModification:
		return conflictResponse()
	default:
		return unhandledErrorResponse(err)
	}
}

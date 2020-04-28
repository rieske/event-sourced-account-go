package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/rieske/event-sourced-account-go/account"
	"github.com/rieske/event-sourced-account-go/eventsourcing"
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
		return r.post(req.Context(), account.ID{accountID}, req.URL.Query())
	case http.MethodGet:
		head, req.URL.Path = shiftPath(req.URL.Path)
		return r.get(req.Context(), head, account.ID{accountID})
	case http.MethodPut:
		head, req.URL.Path = shiftPath(req.URL.Path)
		return r.put(req.Context(), head, account.ID{accountID}, req.URL.Query())
	case http.MethodDelete:
		return r.delete(req.Context(), account.ID{accountID})
	}
	return errorResponse(http.StatusMethodNotAllowed, "method not allowed")
}

func (r *accountResource) post(ctx context.Context, accountID account.ID, query url.Values) response {
	ownerID, response := parseUUID(query.Get("owner"))
	if response != nil {
		return *response
	}

	if err := r.accountService.OpenAccount(ctx, accountID, account.OwnerID{ownerID}); err != nil {
		return handleDomainError(err)
	}

	return locationResponse(http.StatusCreated, "/api/account/"+accountID.String())
}

func (r *accountResource) get(ctx context.Context, action string, id account.ID) response {
	switch action {
	case "":
		return r.queryAccount(ctx, id)
	case "events":
		return r.queryEvents(ctx, id)
	default:
		return actionNotSupported()
	}
}

func actionNotSupported() response {
	return jsonResponse(http.StatusBadRequest, []byte(`{"message":"action not supported"}`))
}

func (r *accountResource) put(ctx context.Context, action string, id account.ID, query url.Values) response {
	switch action {
	case "deposit":
		return r.deposit(ctx, id, query)
	case "withdraw":
		return r.withdraw(ctx, id, query)
	case "transfer":
		return r.transfer(ctx, id, query)
	default:
		return actionNotSupported()
	}
}

func (r *accountResource) queryAccount(ctx context.Context, id account.ID) response {
	snapshot, err := r.accountService.QueryAccount(ctx, id)
	if err != nil {
		return handleDomainError(err)
	}

	response, err := json.Marshal(snapshot)
	if err != nil {
		return unhandledErrorResponse(err)
	}
	return jsonResponse(http.StatusOK, response)
}

func (r *accountResource) queryEvents(ctx context.Context, id account.ID) response {
	events, err := r.accountService.Events(ctx, id)
	if err != nil {
		return handleDomainError(err)
	}

	response, err := json.Marshal(events)
	if err != nil {
		return unhandledErrorResponse(err)
	}
	return jsonResponse(http.StatusOK, response)
}

func (r *accountResource) deposit(ctx context.Context, id account.ID, query url.Values) response {
	amount, response := parseAmount(query.Get("amount"))
	if response != nil {
		return *response
	}
	txId, response := parseUUID(query.Get("transactionId"))
	if response != nil {
		return *response
	}

	err := r.accountService.Deposit(ctx, id, txId, amount)
	return respond(noContentResponse, err)
}

func (r *accountResource) withdraw(ctx context.Context, id account.ID, query url.Values) response {
	amount, response := parseAmount(query.Get("amount"))
	if response != nil {
		return *response
	}
	txId, response := parseUUID(query.Get("transactionId"))
	if response != nil {
		return *response
	}

	err := r.accountService.Withdraw(ctx, id, txId, amount)
	return respond(noContentResponse, err)
}

func (r *accountResource) delete(ctx context.Context, id account.ID) response {
	err := r.accountService.CloseAccount(ctx, id)
	return respond(noContentResponse, err)
}

func (r *accountResource) transfer(ctx context.Context, sourceAccountId account.ID, query url.Values) response {
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

	err := r.accountService.Transfer(ctx, sourceAccountId, account.ID{targetAccountId}, txId, amount)
	return respond(noContentResponse, err)
}

func respond(responseProvider func() response, err error) response {
	if err != nil {
		return handleDomainError(err)
	}
	return responseProvider()
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

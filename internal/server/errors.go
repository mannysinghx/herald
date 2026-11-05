package server

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// apiError is the JSON error body. The codes match ntfy's, because clients branch on them.
type apiError struct {
	Code       int    `json:"code"`
	HTTP       int    `json:"http"`
	Message    string `json:"error"`
	retryAfter int
}

func (e *apiError) Error() string { return e.Message }

func (e *apiError) withDetail(format string, args ...any) *apiError {
	c := *e
	c.Message = e.Message + "; " + fmt.Sprintf(format, args...)
	return &c
}

func (e *apiError) withRetryAfter(seconds int) *apiError {
	c := *e
	c.retryAfter = seconds
	return &c
}

var (
	errEmailDisabled      = &apiError{Code: 40001, HTTP: 400, Message: "e-mail notifications are not enabled"}
	errDelayNoCache       = &apiError{Code: 40002, HTTP: 400, Message: "cannot disable cache for delayed message"}
	errDelayCannotParse   = &apiError{Code: 40004, HTTP: 400, Message: "invalid delay parameter: unable to parse delay"}
	errDelayTooSmall      = &apiError{Code: 40005, HTTP: 400, Message: "invalid delay parameter: too small, please refer to the docs"}
	errDelayTooLarge      = &apiError{Code: 40006, HTTP: 400, Message: "invalid delay parameter: too large, please refer to the docs"}
	errPriorityInvalid    = &apiError{Code: 40007, HTTP: 400, Message: "invalid priority parameter"}
	errSinceInvalid       = &apiError{Code: 40008, HTTP: 400, Message: "invalid since parameter"}
	errTopicInvalid       = &apiError{Code: 40009, HTTP: 400, Message: "invalid request: topic invalid"}
	errTopicDisallowed    = &apiError{Code: 40010, HTTP: 400, Message: "invalid request: topic name is not allowed"}
	errNotUTF8            = &apiError{Code: 40011, HTTP: 400, Message: "invalid request: message must be UTF-8 encoded"}
	errAttachURLInvalid   = &apiError{Code: 40013, HTTP: 400, Message: "invalid request: attachment URL is invalid"}
	errAttachDisallowed   = &apiError{Code: 40014, HTTP: 400, Message: "invalid request: attachments not allowed"}
	errAttachBeforeSend   = &apiError{Code: 40015, HTTP: 400, Message: "invalid request: attachment expiry before delayed delivery date"}
	errWSUpgradeMissing   = &apiError{Code: 40016, HTTP: 400, Message: "invalid request: client not using the websocket protocol"}
	errJSONInvalid        = &apiError{Code: 40024, HTTP: 400, Message: "invalid request: request body must be valid JSON"}
	errActionsInvalid     = &apiError{Code: 40018, HTTP: 400, Message: "invalid request: actions invalid"}
	errIconURLInvalid     = &apiError{Code: 40021, HTTP: 400, Message: "invalid request: icon URL is invalid"}
	errCallsDisabled      = &apiError{Code: 40032, HTTP: 400, Message: "invalid request: calling is disabled"}
	errTemplateInvalid    = &apiError{Code: 40043, HTTP: 400, Message: "invalid request: templates are not supported on this server"}
	errSequenceIDInvalid  = &apiError{Code: 40049, HTTP: 400, Message: "invalid request: sequence ID invalid"}
	errTitleTooLarge      = &apiError{Code: 40057, HTTP: 400, Message: "invalid request: title is too large"}
	errTagsTooLarge       = &apiError{Code: 40058, HTTP: 400, Message: "invalid request: tags are too large"}
	errTooManyTopics      = &apiError{Code: 40061, HTTP: 400, Message: "invalid request: too many topics"}
	errUnauthorized       = &apiError{Code: 40101, HTTP: 401, Message: "unauthorized"}
	errForbidden          = &apiError{Code: 40301, HTTP: 403, Message: "forbidden"}
	errNotFound           = &apiError{Code: 40401, HTTP: 404, Message: "page not found"}
	errAttachTooLarge     = &apiError{Code: 41301, HTTP: 413, Message: "attachment too large, or bandwidth limit reached"}
	errJSONTooLarge       = &apiError{Code: 41303, HTTP: 413, Message: "JSON body too large"}
	errLimitRequests      = &apiError{Code: 42901, HTTP: 429, Message: "limit reached: too many requests"}
	errLimitSubscriptions = &apiError{Code: 42903, HTTP: 429, Message: "limit reached: too many active subscriptions"}
	errLimitBandwidth     = &apiError{Code: 42905, HTTP: 429, Message: "limit reached: daily bandwidth reached"}
	errLimitMessages      = &apiError{Code: 42908, HTTP: 429, Message: "limit reached: daily message quota reached"}
	errLimitAuthFailure   = &apiError{Code: 42909, HTTP: 429, Message: "limit reached: too many auth failures"}
	errInternal           = &apiError{Code: 50001, HTTP: 500, Message: "internal server error"}
)

func writeError(w http.ResponseWriter, e *apiError) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Access-Control-Allow-Origin", "*")
	if e.retryAfter > 0 {
		h.Set("Retry-After", fmt.Sprint(e.retryAfter))
	}
	w.WriteHeader(e.HTTP)
	_ = json.NewEncoder(w).Encode(e)
}

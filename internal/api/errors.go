package api

import (
	"context"
	"encoding/xml"
	"net/http"
)

type Error struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e Error) Error() string {
	return e.Code + ": " + e.Message
}

var (
	ErrNoSuchBucket            = Error{"NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound}
	ErrNoSuchKey               = Error{"NoSuchKey", "The specified key does not exist.", http.StatusNotFound}
	ErrBucketAlreadyOwnedByYou = Error{"BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", http.StatusConflict}
	ErrBucketNotEmpty          = Error{"BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict}
	ErrInvalidBucketName       = Error{"InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest}
	ErrInvalidArgument         = Error{"InvalidArgument", "Invalid argument.", http.StatusBadRequest}
	ErrAccessDenied            = Error{"AccessDenied", "Access Denied.", http.StatusForbidden}
	ErrSignatureDoesNotMatch   = Error{"SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", http.StatusForbidden}
	ErrInvalidAccessKeyID      = Error{"InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.", http.StatusForbidden}
	ErrRequestTimeTooSkewed    = Error{"RequestTimeTooSkewed", "The difference between the request time and the server's time is too large.", http.StatusForbidden}
	ErrAuthorizationMalformed  = Error{"AuthorizationHeaderMalformed", "The authorization header is malformed.", http.StatusBadRequest}
	ErrNoSuchVersion           = Error{"NoSuchVersion", "The specified version does not exist.", http.StatusNotFound}
	ErrInvalidVersioning       = Error{"InvalidArgument", "The versioning status is invalid.", http.StatusBadRequest}
	ErrNoSuchUpload            = Error{"NoSuchUpload", "The specified multipart upload does not exist.", http.StatusNotFound}
	ErrInvalidPart             = Error{"InvalidPart", "One or more of the specified parts could not be found.", http.StatusBadRequest}
	ErrInvalidPartOrder        = Error{"InvalidPartOrder", "The list of parts was not in ascending order.", http.StatusBadRequest}
	ErrNotImplemented          = Error{"NotImplemented", "A header you provided implies functionality that is not implemented.", http.StatusNotImplemented}
	ErrMethodNotAllowed        = Error{"MethodNotAllowed", "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed}
	ErrInternalError           = Error{"InternalError", "We encountered an internal error, please try again.", http.StatusInternalServerError}
)

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

func WriteError(w http.ResponseWriter, r *http.Request, e Error) {
	writeXML(w, e.HTTPStatus, errorResponse{
		Code:      e.Code,
		Message:   e.Message,
		Resource:  r.URL.Path,
		RequestID: GetRequestID(r.Context()),
	})
}

type contextKey string

const requestIDKey contextKey = "gos3-request-id"

func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

func GetRequestID(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

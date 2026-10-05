package sign

import (
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/kxj/gos3/internal/auth"
)

// PresignGetURL builds an AWS SigV4 presigned GET URL for the given path.
func PresignGetURL(scheme, host, path string, creds auth.Credentials, region string, now time.Time, expiry time.Duration) string {
	const service = "s3"
	now = now.UTC()
	date := now.Format("20060102")
	amzDate := now.Format(iso8601Layout)
	scope := date + "/" + region + "/" + service + "/aws4_request"

	q := url.Values{}
	q.Set("X-Amz-Algorithm", Algorithm)
	q.Set("X-Amz-Credential", creds.AccessKey+"/"+scope)
	q.Set("X-Amz-Date", amzDate)
	q.Set("X-Amz-Expires", strconv.Itoa(int(expiry/time.Second)))
	q.Set("X-Amz-SignedHeaders", "host")
	rawQuery := q.Encode()

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Path: path, RawQuery: rawQuery},
		Host:   host,
		Header: http.Header{},
	}
	signed := []string{"host"}
	creq := canonicalRequestPresigned(req, signed, UnsignedPayload)
	sts := stringToSign(amzDate, scope, creq)
	key := signingKey(creds.SecretKey, date, region, service)
	sig := hex.EncodeToString(hmacSHA256(key, sts))

	return scheme + "://" + host + uriEncode(path, false) + "?" + rawQuery + "&X-Amz-Signature=" + sig
}

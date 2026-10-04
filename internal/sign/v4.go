package sign

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kxj/gos3/internal/auth"
)

const (
	Algorithm       = "AWS4-HMAC-SHA256"
	StreamingPolicy = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	UnsignedPayload = "UNSIGNED-PAYLOAD"
	EmptySHA256     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	iso8601Layout   = "20060102T150405Z"
)

var (
	ErrMissingAuth        = errors.New("missing authorization")
	ErrInvalidAuth        = errors.New("invalid authorization")
	ErrAccessDenied       = errors.New("access denied")
	ErrSignature          = errors.New("signature mismatch")
	ErrRequestExpired     = errors.New("request time expired")
	ErrUnsupportedPayload = errors.New("unsupported payload hashing")
)

type CredentialsProvider interface {
	Get(accessKey string) (auth.Credentials, bool)
}

type Result struct {
	Credentials auth.Credentials
	Streaming   bool
	SigningKey  []byte
	Scope       string
	AmzDate     string
	Signature   string
}

type parsedAuth struct {
	accessKey string
	scope     string
	date      string
	region    string
	service   string
	signed    []string
	signature string
}

func VerifyHeader(r *http.Request, provider CredentialsProvider, now time.Time, skew time.Duration) (Result, error) {
	a, err := parseAuthHeader(r.Header.Get("Authorization"))
	if err != nil {
		return Result{}, err
	}
	creds, ok := provider.Get(a.accessKey)
	if !ok {
		return Result{}, ErrAccessDenied
	}
	amzDate := r.Header.Get("x-amz-date")
	if amzDate == "" {
		return Result{}, ErrInvalidAuth
	}
	t, err := time.Parse(iso8601Layout, amzDate)
	if err != nil {
		return Result{}, ErrInvalidAuth
	}
	if skew > 0 && (now.Sub(t) > skew || t.Sub(now) > skew) {
		return Result{}, ErrRequestExpired
	}

	payloadHash := r.Header.Get("x-amz-content-sha256")
	streaming := false
	switch {
	case strings.HasPrefix(payloadHash, "STREAMING-"):
		if payloadHash != StreamingPolicy {
			return Result{}, ErrUnsupportedPayload
		}
		streaming = true
	case payloadHash == "":
		if r.Body == nil || r.ContentLength == 0 {
			payloadHash = EmptySHA256
		} else {
			return Result{}, ErrUnsupportedPayload
		}
	}

	signed := normalizeSigned(a.signed)
	creq := canonicalRequest(r, signed, payloadHash)
	scope := a.scope
	sts := stringToSign(amzDate, scope, creq)
	key := signingKey(creds.SecretKey, a.date, a.region, a.service)
	sig := hex.EncodeToString(hmacSHA256(key, sts))
	if subtle.ConstantTimeCompare([]byte(sig), []byte(strings.ToLower(a.signature))) != 1 {
		return Result{}, ErrSignature
	}

	return Result{
		Credentials: creds,
		Streaming:   streaming,
		SigningKey:  key,
		Scope:       scope,
		AmzDate:     amzDate,
		Signature:   sig,
	}, nil
}

func VerifyQuery(r *http.Request, provider CredentialsProvider, now time.Time, maxExpiry time.Duration) (Result, error) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != Algorithm {
		return Result{}, ErrMissingAuth
	}
	segs := strings.Split(q.Get("X-Amz-Credential"), "/")
	if len(segs) != 5 || segs[4] != "aws4_request" {
		return Result{}, ErrInvalidAuth
	}
	accessKey, date, region, service := segs[0], segs[1], segs[2], segs[3]
	creds, ok := provider.Get(accessKey)
	if !ok {
		return Result{}, ErrAccessDenied
	}

	amzDate := q.Get("X-Amz-Date")
	t, err := time.Parse(iso8601Layout, amzDate)
	if err != nil {
		return Result{}, ErrInvalidAuth
	}
	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires < 0 {
		return Result{}, ErrInvalidAuth
	}
	if maxExpiry > 0 && time.Duration(expires)*time.Second > maxExpiry {
		return Result{}, ErrRequestExpired
	}
	if now.After(t.Add(time.Duration(expires) * time.Second)) {
		return Result{}, ErrRequestExpired
	}

	signature := q.Get("X-Amz-Signature")
	if signature == "" {
		return Result{}, ErrInvalidAuth
	}
	signed := normalizeSigned(strings.Split(q.Get("X-Amz-SignedHeaders"), ";"))
	scope := strings.Join(segs[1:], "/")
	creq := canonicalRequestPresigned(r, signed, UnsignedPayload)
	sts := stringToSign(amzDate, scope, creq)
	key := signingKey(creds.SecretKey, date, region, service)
	sig := hex.EncodeToString(hmacSHA256(key, sts))
	if subtle.ConstantTimeCompare([]byte(sig), []byte(strings.ToLower(signature))) != 1 {
		return Result{}, ErrSignature
	}
	return Result{Credentials: creds, SigningKey: key, Scope: scope, AmzDate: amzDate, Signature: sig}, nil
}

func parseAuthHeader(header string) (*parsedAuth, error) {
	if header == "" {
		return nil, ErrMissingAuth
	}
	if !strings.HasPrefix(header, Algorithm) {
		return nil, ErrInvalidAuth
	}
	a := &parsedAuth{}
	rest := strings.TrimSpace(strings.TrimPrefix(header, Algorithm))
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return nil, ErrInvalidAuth
		}
		switch strings.TrimSpace(key) {
		case "Credential":
			segs := strings.Split(value, "/")
			if len(segs) != 5 || segs[4] != "aws4_request" {
				return nil, ErrInvalidAuth
			}
			a.accessKey, a.date, a.region, a.service = segs[0], segs[1], segs[2], segs[3]
			a.scope = strings.Join(segs[1:], "/")
		case "SignedHeaders":
			a.signed = strings.Split(value, ";")
		case "Signature":
			a.signature = value
		}
	}
	if a.accessKey == "" || len(a.signed) == 0 || a.signature == "" {
		return nil, ErrInvalidAuth
	}
	return a, nil
}

func canonicalRequest(r *http.Request, signed []string, payloadHash string) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(uriEncode(r.URL.Path, false))
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(r.URL.RawQuery, false))
	b.WriteByte('\n')
	b.WriteString(canonicalHeaders(r, signed))
	b.WriteByte('\n')
	b.WriteString(strings.Join(signed, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String()
}

func canonicalRequestPresigned(r *http.Request, signed []string, payloadHash string) string {
	var b strings.Builder
	b.WriteString(r.Method)
	b.WriteByte('\n')
	b.WriteString(uriEncode(r.URL.Path, false))
	b.WriteByte('\n')
	b.WriteString(canonicalQuery(r.URL.RawQuery, true))
	b.WriteByte('\n')
	b.WriteString(canonicalHeaders(r, signed))
	b.WriteByte('\n')
	b.WriteString(strings.Join(signed, ";"))
	b.WriteByte('\n')
	b.WriteString(payloadHash)
	return b.String()
}

func canonicalQuery(rawQuery string, excludeSignature bool) string {
	if rawQuery == "" {
		return ""
	}
	type pair struct{ k, v string }
	pairs := make([]pair, 0, strings.Count(rawQuery, "&")+1)
	for _, p := range strings.Split(rawQuery, "&") {
		if p == "" {
			continue
		}
		k, v, _ := strings.Cut(p, "=")
		dk, err := url.QueryUnescape(k)
		if err != nil {
			dk = k
		}
		dv, err := url.QueryUnescape(v)
		if err != nil {
			dv = v
		}
		if excludeSignature && dk == "X-Amz-Signature" {
			continue
		}
		pairs = append(pairs, pair{uriEncode(dk, true), uriEncode(dv, true)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k == pairs[j].k {
			return pairs[i].v < pairs[j].v
		}
		return pairs[i].k < pairs[j].k
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

func canonicalHeaders(r *http.Request, signed []string) string {
	var b strings.Builder
	for _, h := range signed {
		var vals []string
		switch h {
		case "host":
			vals = []string{r.Host}
		case "content-length":
			if r.ContentLength >= 0 {
				vals = []string{strconv.FormatInt(r.ContentLength, 10)}
			}
		default:
			vals = r.Header.Values(h)
		}
		for i := range vals {
			vals[i] = trimHeaderValue(vals[i])
		}
		b.WriteString(h)
		b.WriteByte(':')
		b.WriteString(strings.Join(vals, ","))
		b.WriteByte('\n')
	}
	return b.String()
}

func stringToSign(amzDate, scope, canonicalRequest string) string {
	sum := sha256.Sum256([]byte(canonicalRequest))
	return Algorithm + "\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

func normalizeSigned(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, h := range in {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

func trimHeaderValue(v string) string {
	return strings.Join(strings.Fields(v), " ")
}

func uriEncode(s string, encodeSlash bool) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&0x0f])
		}
	}
	return b.String()
}

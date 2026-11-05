package submission

import (
	"bytes"
	"net/mail"
	"strings"
)

const (
	maxFormIDLen = 255
	maxKeyLen    = 255
	maxEmailLen  = 254 // RFC 5321 path limit
)

// Validate normalises req in place and returns field -> message for every
// problem found. An empty map means the request is valid.
func Validate(req *CreateRequest) map[string]string {
	errs := map[string]string{}

	req.FormID = strings.TrimSpace(req.FormID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.Email = strings.TrimSpace(req.Email)

	switch {
	case req.FormID == "":
		errs["form_id"] = "is required"
	case len(req.FormID) > maxFormIDLen:
		errs["form_id"] = "is too long"
	}

	switch {
	case req.IdempotencyKey == "":
		errs["idempotency_key"] = "is required"
	case len(req.IdempotencyKey) > maxKeyLen:
		errs["idempotency_key"] = "is too long"
	}

	if !isValidEmail(req.Email) {
		errs["email"] = "must be a valid email address"
	}

	data := bytes.TrimSpace(req.Data)
	switch {
	case len(data) == 0 || bytes.Equal(data, []byte("null")):
		req.Data = []byte("{}")
	case data[0] != '{':
		errs["data"] = "must be a JSON object"
	}

	return errs
}

// isValidEmail accepts a bare address only. mail.ParseAddress alone would
// also accept display-name forms like "John <john@example.com>", so we
// require the parsed address to equal the input, plus a dotted domain.
func isValidEmail(s string) bool {
	if s == "" || len(s) > maxEmailLen {
		return false
	}
	addr, err := mail.ParseAddress(s)
	if err != nil || addr.Address != s {
		return false
	}
	at := strings.LastIndexByte(s, '@')
	domain := s[at+1:]
	return strings.Contains(domain, ".") && !strings.HasPrefix(domain, ".") && !strings.HasSuffix(domain, ".")
}

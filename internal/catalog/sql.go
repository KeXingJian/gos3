package catalog

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type SQLQuery struct {
	Columns      []string
	Count        bool
	Filter       Filter
	Alternatives []Filter
	RequiredTags map[string]string
	RequiredMeta map[string]string
}
type sqlToken struct {
	text string
	kind byte
}
type sqlParser struct {
	tokens []sqlToken
	pos    int
}

func ParseSQL(input string) (SQLQuery, error) {
	var out SQLQuery
	tokens, err := lexSQL(input)
	if err != nil {
		return out, ErrInvalidFilter
	}
	p := sqlParser{tokens: tokens}
	if !p.keyword("SELECT") {
		return out, ErrInvalidFilter
	}
	if p.take("*") {
		out.Columns = []string{"*"}
	} else if p.keyword("COUNT") {
		if !p.take("(") || !p.take("*") || !p.take(")") {
			return out, ErrInvalidFilter
		}
		out.Count = true
		out.Columns = []string{"count(*)"}
	} else {
		for {
			col, ok := p.identifier()
			if !ok {
				return out, ErrInvalidFilter
			}
			col = normalizeSQLField(col)
			if !validSQLColumn(strings.ToLower(col)) {
				return out, ErrInvalidFilter
			}
			out.Columns = append(out.Columns, col)
			if !p.take(",") {
				break
			}
		}
	}
	if !p.keyword("FROM") {
		return out, ErrInvalidFilter
	}
	table, ok := p.identifier()
	if !ok || !strings.EqualFold(table, "S3Object") {
		return out, ErrInvalidFilter
	}
	base := Filter{}
	inField := ""
	var inValues []string
	var requiredTags, requiredMeta map[string]string
	if p.keyword("WHERE") {
		for {
			field, ok := p.identifier()
			if !ok {
				return out, ErrInvalidFilter
			}
			field = normalizeSQLField(field)
			fieldLower := strings.ToLower(field)
			if !validFilterField(fieldLower) {
				return out, ErrInvalidFilter
			}
			if p.keyword("IN") {
				if inField != "" || !p.take("(") {
					return out, ErrInvalidFilter
				}
				inField = fieldLower
				for {
					v, ok := p.value()
					if !ok {
						return out, ErrInvalidFilter
					}
					inValues = append(inValues, v)
					if p.take(")") {
						break
					}
					if !p.take(",") {
						return out, ErrInvalidFilter
					}
				}
				if len(inValues) == 0 || len(inValues) > 10 || field == "size" || field == "modtime" {
					return out, ErrInvalidFilter
				}
			} else {
				op, ok := p.operator()
				if !ok {
					return out, ErrInvalidFilter
				}
				v, ok := p.value()
				if !ok {
					return out, ErrInvalidFilter
				}
				if strings.HasPrefix(fieldLower, "tag.") && op == "=" {
					k := field[4:]
					if existing, ok := requiredTags[k]; ok && existing != v {
						return out, ErrInvalidFilter
					}
					if requiredTags == nil {
						requiredTags = map[string]string{}
					}
					requiredTags[k] = v
				}
				if strings.HasPrefix(fieldLower, "meta.") && op == "=" {
					k := field[5:]
					if existing, ok := requiredMeta[k]; ok && existing != v {
						return out, ErrInvalidFilter
					}
					if requiredMeta == nil {
						requiredMeta = map[string]string{}
					}
					requiredMeta[k] = v
				}
				if err := addSQLPredicate(&base, field, op, v); err != nil {
					return out, err
				}
			}
			if !p.keyword("AND") {
				break
			}
		}
		if p.keyword("OR") {
			return out, ErrInvalidFilter
		}
	}
	if p.keyword("LIMIT") {
		v, ok := p.value()
		if !ok {
			return out, ErrInvalidFilter
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > MaxLimit {
			return out, ErrInvalidFilter
		}
		base.Limit = n
	}
	if p.pos != len(tokens) {
		return out, ErrInvalidFilter
	}
	if base.Limit == 0 {
		base.Limit = DefaultLimit
	}
	out.Filter = base
	out.RequiredTags = requiredTags
	out.RequiredMeta = requiredMeta
	if inField != "" {
		if inField == "key" {
			if base.Key != "" || base.Prefix != "" {
				return SQLQuery{}, ErrInvalidFilter
			}
		}
		if strings.HasPrefix(strings.ToLower(inField), "tag.") {
			key := inField[4:]
			if old, ok := requiredTags[key]; ok {
				for _, v := range inValues {
					if v != old {
						return SQLQuery{}, ErrInvalidFilter
					}
				}
			}
		}
		if strings.HasPrefix(strings.ToLower(inField), "meta.") {
			key := inField[5:]
			if old, ok := requiredMeta[key]; ok {
				for _, v := range inValues {
					if v != old {
						return SQLQuery{}, ErrInvalidFilter
					}
				}
			}
		}
		for _, v := range inValues {
			f := base
			switch {
			case inField == "key":
				f.Key = v
			case strings.HasPrefix(strings.ToLower(inField), "tag."):
				if f.Tag == nil {
					f.Tag = map[string]string{}
				}
				f.Tag[inField[4:]] = v
			case strings.HasPrefix(strings.ToLower(inField), "meta."):
				if f.Meta == nil {
					f.Meta = map[string]string{}
				}
				f.Meta[inField[5:]] = v
			default:
				if err := addSQLPredicate(&f, inField, "=", v); err != nil {
					return SQLQuery{}, err
				}
			}
			out.Alternatives = append(out.Alternatives, f)
		}
	}
	return out, nil
}
func addSQLPredicate(f *Filter, field, op, value string) error {
	if field == "key" && strings.EqualFold(op, "LIKE") {
		if !strings.HasSuffix(value, "%") || strings.Contains(strings.TrimSuffix(value, "%"), "%") || strings.Contains(value, "_") {
			return ErrInvalidFilter
		}
		f.Prefix = strings.TrimSuffix(value, "%")
		return nil
	}
	switch field {
	case "bucket":
		if op != "=" {
			return ErrInvalidFilter
		}
		f.Bucket = value
	case "key":
		if op != "=" {
			return ErrInvalidFilter
		}
		f.Key = value
	case "contenttype":
		if op != "=" {
			return ErrInvalidFilter
		}
		f.ContentType = value
	case "size":
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 0 {
			return ErrInvalidFilter
		}
		switch op {
		case "=":
			if err := setMinSize(f, n); err != nil {
				return err
			}
			return setMaxSize(f, n)
		case ">":
			if n == int64(^uint64(0)>>1) {
				return ErrInvalidFilter
			}
			return setMinSize(f, n+1)
		case ">=":
			return setMinSize(f, n)
		case "<":
			if n == 0 {
				return ErrInvalidFilter
			}
			return setMaxSize(f, n-1)
		case "<=":
			return setMaxSize(f, n)
		default:
			return ErrInvalidFilter
		}
	case "modtime":
		if op != "=" && op != ">" && op != ">=" && op != "<" && op != "<=" {
			return ErrInvalidFilter
		}
		t, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return ErrInvalidFilter
		}
		switch op {
		case ">", ">=":
			f.After = t
		case "<", "<=":
			f.Before = t
		case "=":
			f.After = t
			f.Before = t.Add(time.Nanosecond)
		}
	default:
		if strings.HasPrefix(strings.ToLower(field), "meta.") && op == "=" {
			field = "meta." + strings.ToLower(field[5:])
			if f.Meta == nil {
				f.Meta = map[string]string{}
			}
			f.Meta[field[5:]] = value
		} else if strings.HasPrefix(strings.ToLower(field), "tag.") && op == "=" {
			if f.Tag == nil {
				f.Tag = map[string]string{}
			}
			f.Tag[field[4:]] = value
		} else {
			return ErrInvalidFilter
		}
	}
	return nil
}
func setMinSize(f *Filter, n int64) error {
	if !f.MinSizeSet || n > f.MinSize {
		f.MinSize = n
		f.MinSizeSet = true
	}
	if f.MaxSizeSet && f.MinSize > f.MaxSize {
		return ErrInvalidFilter
	}
	return nil
}
func setMaxSize(f *Filter, n int64) error {
	if !f.MaxSizeSet || n < f.MaxSize {
		f.MaxSize = n
		f.MaxSizeSet = true
	}
	if f.MinSizeSet && f.MinSize > f.MaxSize {
		return ErrInvalidFilter
	}
	return nil
}
func normalizeSQLField(s string) string {
	low := strings.ToLower(s)
	if strings.HasPrefix(low, "meta.") {
		return "meta." + strings.ToLower(s[5:])
	}
	if strings.HasPrefix(low, "tag.") {
		return "tag." + s[4:]
	}
	return low
}

func validSQLColumn(s string) bool {
	switch s {
	case "*", "bucket", "key", "size", "etag", "contenttype", "modtime", "versionid", "islatest", "deletemarker":
		return true
	}
	return strings.HasPrefix(s, "meta.") && len(s) > 5 || strings.HasPrefix(s, "tag.") && len(s) > 4
}
func validFilterField(s string) bool {
	return s == "bucket" || s == "key" || s == "size" || s == "contenttype" || s == "modtime" || strings.HasPrefix(s, "meta.") && len(s) > 5 || strings.HasPrefix(s, "tag.") && len(s) > 4
}
func (p *sqlParser) take(s string) bool {
	if p.pos < len(p.tokens) && strings.EqualFold(p.tokens[p.pos].text, s) {
		p.pos++
		return true
	}
	return false
}
func (p *sqlParser) keyword(s string) bool {
	if p.pos < len(p.tokens) && p.tokens[p.pos].kind == 'i' && strings.EqualFold(p.tokens[p.pos].text, s) {
		p.pos++
		return true
	}
	return false
}
func (p *sqlParser) identifier() (string, bool) {
	if p.pos >= len(p.tokens) || p.tokens[p.pos].kind != 'i' {
		return "", false
	}
	s := p.tokens[p.pos].text
	p.pos++
	return s, true
}
func (p *sqlParser) value() (string, bool) {
	if p.pos >= len(p.tokens) {
		return "", false
	}
	t := p.tokens[p.pos]
	if t.kind != 's' && t.kind != 'n' {
		return "", false
	}
	p.pos++
	return t.text, true
}
func (p *sqlParser) operator() (string, bool) {
	if p.keyword("LIKE") {
		return "LIKE", true
	}
	if p.pos >= len(p.tokens) {
		return "", false
	}
	s := p.tokens[p.pos].text
	switch s {
	case "=", ">", "<", ">=", "<=":
		p.pos++
		return s, true
	}
	return "", false
}
func lexSQL(s string) ([]sqlToken, error) {
	var out []sqlToken
	for i := 0; i < len(s); {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			i++
			continue
		}
		if c == ';' {
			i++
			for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == '\n') {
				i++
			}
			if i != len(s) {
				return nil, fmt.Errorf("trailing SQL")
			}
			break
		}
		if c == '\'' {
			i++
			var b strings.Builder
			closed := false
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				b.WriteByte(s[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated string")
			}
			out = append(out, sqlToken{b.String(), 's'})
			continue
		}
		if strings.ContainsRune(",()*=<>", rune(c)) {
			j := i + 1
			if (c == '>' || c == '<') && j < len(s) && s[j] == '=' {
				j++
			}
			out = append(out, sqlToken{s[i:j], 'p'})
			i = j
			continue
		}
		if c >= '0' && c <= '9' {
			j := i + 1
			for j < len(s) && s[j] >= '0' && s[j] <= '9' {
				j++
			}
			out = append(out, sqlToken{s[i:j], 'n'})
			i = j
			continue
		}
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' {
			j := i + 1
			for j < len(s) {
				x := s[j]
				if !(x >= 'a' && x <= 'z' || x >= 'A' && x <= 'Z' || x >= '0' && x <= '9' || x == '_' || x == '.') {
					break
				}
				j++
			}
			out = append(out, sqlToken{s[i:j], 'i'})
			i = j
			continue
		}
		return nil, fmt.Errorf("invalid SQL character")
	}
	return out, nil
}

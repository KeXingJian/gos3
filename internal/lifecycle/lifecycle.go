package lifecycle

import (
	"encoding/xml"
	"errors"
	"strings"
	"time"
)

var ErrInvalidExpiration = errors.New("lifecycle: rule needs Expiration Days or Date")

type Configuration struct {
	XMLName xml.Name `xml:"LifecycleConfiguration" json:"-"`
	Xmlns   string   `xml:"xmlns,attr" json:"-"`
	Rules   []Rule   `xml:"Rule" json:"Rules"`
}

type Rule struct {
	ID         string     `xml:"ID,omitempty" json:"ID,omitempty"`
	Status     string     `xml:"Status" json:"Status"`
	Filter     Filter     `xml:"Filter" json:"Filter"`
	Expiration Expiration `xml:"Expiration" json:"Expiration"`
}

type Filter struct {
	Prefix string `xml:"Prefix,omitempty" json:"Prefix,omitempty"`
}

type Expiration struct {
	Days int    `xml:"Days,omitempty" json:"Days,omitempty"`
	Date string `xml:"Date,omitempty" json:"Date,omitempty"`
}

func (c Configuration) Validate() error {
	for _, r := range c.Rules {
		if r.Expiration.Days <= 0 && r.Expiration.Date == "" {
			return ErrInvalidExpiration
		}
	}
	return nil
}

func (c Configuration) Expired(key string, modTime, now time.Time) bool {
	for _, r := range c.Rules {
		if !strings.EqualFold(r.Status, "Enabled") {
			continue
		}
		if r.Filter.Prefix != "" && !strings.HasPrefix(key, r.Filter.Prefix) {
			continue
		}
		if r.Expiration.Days > 0 {
			if !modTime.Add(time.Duration(r.Expiration.Days) * 24 * time.Hour).After(now) {
				return true
			}
		}
		if r.Expiration.Date != "" {
			if t, err := time.Parse(time.RFC3339, r.Expiration.Date); err == nil && !t.After(now) {
				return true
			}
		}
	}
	return false
}

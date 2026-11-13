package catalog

import (
	"errors"
	"testing"
)

func TestParseSQLSubset(t *testing.T) {
	q, err := ParseSQL("SELECT key, size, meta.owner FROM S3Object WHERE bucket = 'photos' AND size >= 100 AND key LIKE '2026/%' LIMIT 25")
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Columns) != 3 || q.Filter.Bucket != "photos" || q.Filter.MinSize != 100 || q.Filter.Prefix != "2026/" || q.Filter.Limit != 25 {
		t.Fatalf("parsed query = %+v", q)
	}
	q, err = ParseSQL("SELECT count(*) FROM S3Object WHERE tag.env IN ('prod', 'stage')")
	if err != nil {
		t.Fatal(err)
	}
	if !q.Count || len(q.Alternatives) != 2 || q.Alternatives[0].Tag["env"] != "prod" || q.Filter.Tag["env"] != "" || q.RequiredTags["env"] != "" {
		t.Fatalf("parsed IN query = %+v", q)
	}
	q, err = ParseSQL("SELECT key FROM S3Object WHERE bucket = 'b' AND tag.env = 'prod'")
	if err != nil || q.Filter.Tag["env"] != "prod" || q.RequiredTags["env"] != "prod" || q.Count {
		t.Fatalf("tag query = %+v err=%v", q, err)
	}
	q, err = ParseSQL("SELECT meta.Owner, tag.Env FROM S3Object WHERE meta.Owner = 'alice'")
	if err != nil || q.Columns[0] != "meta.owner" || q.Columns[1] != "tag.Env" || q.RequiredMeta["owner"] != "alice" {
		t.Fatalf("case-sensitive metadata key query = %+v err=%v", q, err)
	}
}

func TestParseSQLRejectsUnsafeOrUnsupported(t *testing.T) {
	for _, input := range []string{
		"DELETE FROM S3Object", "SELECT key FROM S3Object; DROP TABLE x", "SELECT key FROM S3Object WHERE key LIKE '%foo'",
		"SELECT key FROM S3Object WHERE size >= -1", "SELECT key FROM S3Object WHERE bucket = 'b' OR size = 1",
		"SELECT key FROM S3Object LIMIT 1001", "SELECT key FROM Other",
	} {
		if _, err := ParseSQL(input); !errors.Is(err, ErrInvalidFilter) {
			t.Errorf("ParseSQL(%q) err=%v, want invalid filter", input, err)
		}
	}
}

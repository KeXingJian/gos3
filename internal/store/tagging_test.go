package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestFSObjectTaggingPerVersion(t *testing.T) {
	ctx := context.Background()
	fs, err := NewFS(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}
	if err := fs.SetBucketVersioning(ctx, "bucket", VersioningEnabled); err != nil {
		t.Fatal(err)
	}
	v1, err := fs.PutObject(ctx, "bucket", "key", strings.NewReader("one"), 3, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.PutObjectTagging(ctx, "bucket", "key", v1.VersionID, map[string]string{"env": "prod"}); err != nil {
		t.Fatal(err)
	}
	got, err := fs.GetObjectTagging(ctx, "bucket", "key", v1.VersionID)
	if err != nil || got["env"] != "prod" {
		t.Fatalf("tags=%v err=%v", got, err)
	}
	v2, err := fs.PutObject(ctx, "bucket", "key", strings.NewReader("two"), 3, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := fs.PutObjectTagging(ctx, "bucket", "key", v2.VersionID, map[string]string{"env": "dev"}); err != nil {
		t.Fatal(err)
	}
	old, _ := fs.GetObjectTagging(ctx, "bucket", "key", v1.VersionID)
	latest, _ := fs.GetObjectTagging(ctx, "bucket", "key", v2.VersionID)
	if old["env"] != "prod" || latest["env"] != "dev" {
		t.Fatalf("version tags old=%v latest=%v", old, latest)
	}
	if err := fs.DeleteObjectTagging(ctx, "bucket", "key", v2.VersionID); err != nil {
		t.Fatal(err)
	}
	got, err = fs.GetObjectTagging(ctx, "bucket", "key", v2.VersionID)
	if err != nil || len(got) != 0 {
		t.Fatalf("deleted tags=%v err=%v", got, err)
	}
	if err := fs.PutObjectTagging(ctx, "bucket", "key", "missing", nil); !errors.Is(err, ErrNoSuchVersion) {
		t.Fatalf("missing version err=%v", err)
	}
}

package main

import (
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"testing"

	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := initDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedPost(t *testing.T, db *sql.DB, uri, cid, authorDID string, waveform *string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO posts (uri, cid, author_did, blob_cid, mime_type, duration, waveform, reply_root, reply_parent, created_at, indexed_at)
		VALUES (?, ?, ?, 'bafkfake', 'audio/webm', 12345, ?, NULL, NULL, '2026-04-15T12:00:00Z', '2026-04-15T12:00:01Z')
	`, uri, cid, authorDID, waveform)
	if err != nil {
		t.Fatal(err)
	}
}

func seedLike(t *testing.T, db *sql.DB, likeURI, subjectURI string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO likes (uri, cid, author_did, subject_uri, subject_cid, created_at, indexed_at)
		VALUES (?, 'bafylike', 'did:plc:liker', ?, 'bafycid', '2026-04-15T13:00:00Z', '2026-04-15T13:00:01Z')
	`, likeURI, subjectURI)
	if err != nil {
		t.Fatal(err)
	}
}

func callGetPostThread(t *testing.T, db *sql.DB, query string) *httptest.ResponseRecorder {
	t.Helper()
	handler := handleGetPostThread(db)
	req := httptest.NewRequest("GET", "/xrpc/at.yaps.audio.getPostThread?"+query, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestGetPostThread_ExistingPost(t *testing.T) {
	db := setupTestDB(t)
	postURI := "at://did:plc:abc123/at.yaps.audio.post/3kxyz"
	wf := "[0,12,45]"
	seedPost(t, db, postURI, "bafyabc", "did:plc:abc123", &wf)
	seedLike(t, db, "at://did:plc:liker/at.yaps.audio.like/1", postURI)
	seedLike(t, db, "at://did:plc:liker/at.yaps.audio.like/2", postURI)

	rec := callGetPostThread(t, db, "uri="+postURI)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]json.RawMessage
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var thread threadViewPost
	if err := json.Unmarshal(resp["thread"], &thread); err != nil {
		t.Fatalf("failed to unmarshal thread: %v", err)
	}

	if thread.Post.URI != postURI {
		t.Errorf("uri = %q, want %q", thread.Post.URI, postURI)
	}
	if thread.Post.AuthorDID != "did:plc:abc123" {
		t.Errorf("author_did = %q, want did:plc:abc123", thread.Post.AuthorDID)
	}
	if thread.Post.LikeCount != 2 {
		t.Errorf("like_count = %d, want 2", thread.Post.LikeCount)
	}
	if len(thread.Post.Waveform) != 3 {
		t.Errorf("waveform len = %d, want 3", len(thread.Post.Waveform))
	}
	if len(thread.Replies) != 0 {
		t.Errorf("replies len = %d, want 0 (v1 depth=0)", len(thread.Replies))
	}
}

func TestGetPostThread_NotFound(t *testing.T) {
	db := setupTestDB(t)
	uri := "at://did:plc:abc123/at.yaps.audio.post/nonexistent"

	rec := callGetPostThread(t, db, "uri="+uri)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]json.RawMessage
	json.Unmarshal(rec.Body.Bytes(), &resp)

	var nf notFoundPost
	if err := json.Unmarshal(resp["thread"], &nf); err != nil {
		t.Fatalf("failed to unmarshal notFoundPost: %v", err)
	}
	if !nf.NotFound {
		t.Error("expected notFound=true")
	}
	if nf.URI != uri {
		t.Errorf("uri = %q, want %q", nf.URI, uri)
	}
}

func TestGetPostThread_MalformedURI(t *testing.T) {
	db := setupTestDB(t)

	cases := []string{
		"",
		"not-a-uri",
		"https://example.com",
		"at://did:plc:abc/at.yaps.audio.post",
		"at://did:plc:abc/at.yaps.audio.post/",
		"at:///at.yaps.audio.post/rkey",
	}

	for _, uri := range cases {
		rec := callGetPostThread(t, db, "uri="+uri)
		if rec.Code != 400 {
			t.Errorf("uri=%q: expected 400, got %d", uri, rec.Code)
		}
	}
}

func TestGetPostThread_WrongCollection(t *testing.T) {
	db := setupTestDB(t)
	rec := callGetPostThread(t, db, "uri=at://did:plc:abc/at.yaps.audio.like/3kxyz")

	if rec.Code != 400 {
		t.Fatalf("expected 400 for like URI, got %d", rec.Code)
	}
}

func TestGetPostThread_NullWaveform(t *testing.T) {
	db := setupTestDB(t)
	postURI := "at://did:plc:abc123/at.yaps.audio.post/nowf"
	seedPost(t, db, postURI, "bafyabc", "did:plc:abc123", nil)

	rec := callGetPostThread(t, db, "uri="+postURI)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	var raw map[string]map[string]map[string]json.RawMessage
	json.Unmarshal([]byte(body), &raw)

	if _, exists := raw["thread"]["post"]["waveform"]; exists {
		t.Error("waveform should be omitted when null, not present")
	}
}

func TestGetPostThread_DepthIgnored(t *testing.T) {
	db := setupTestDB(t)
	postURI := "at://did:plc:abc123/at.yaps.audio.post/depthtest"
	seedPost(t, db, postURI, "bafyabc", "did:plc:abc123", nil)

	rec0 := callGetPostThread(t, db, "uri="+postURI+"&depth=0")
	rec5 := callGetPostThread(t, db, "uri="+postURI+"&depth=5")

	if rec0.Body.String() != rec5.Body.String() {
		t.Error("depth=0 and depth=5 should return identical responses in v1")
	}
}

func TestGetPostThread_CORSHeader(t *testing.T) {
	db := setupTestDB(t)
	postURI := "at://did:plc:abc123/at.yaps.audio.post/cors"
	seedPost(t, db, postURI, "bafyabc", "did:plc:abc123", nil)

	rec := callGetPostThread(t, db, "uri="+postURI)
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS header on success response")
	}

	rec400 := callGetPostThread(t, db, "uri=bad")
	if rec400.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS header on error response")
	}
}

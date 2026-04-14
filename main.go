package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// Jetstream event types
// ---------------------------------------------------------------------------

type JetstreamEvent struct {
	Did        string          `json:"did"`
	TimeUS     int64           `json:"time_us"`
	Kind       string          `json:"kind"`       // "commit", "identity", "account"
	Commit     *CommitPayload  `json:"commit"`
}

type CommitPayload struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"`  // "create", "update", "delete"
	Collection string          `json:"collection"`
	Rkey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record"`
	CID        string          `json:"cid"`
}

type AudioPostRecord struct {
	Type      string    `json:"$type"`
	Duration  int       `json:"duration"`
	Waveform  []int     `json:"waveform,omitempty"`
	CreatedAt string    `json:"createdAt"`
	Reply     *ReplyRef `json:"reply,omitempty"`
	Audio     BlobRef   `json:"audio"`
}

type ReplyRef struct {
	Root   StrongRef `json:"root"`
	Parent StrongRef `json:"parent"`
}

type StrongRef struct {
	URI string `json:"uri"`
	CID string `json:"cid"`
}

type BlobRef struct {
	Type     string   `json:"$type"`
	Ref      CIDLink  `json:"ref"`
	MimeType string   `json:"mimeType"`
	Size     int      `json:"size"`
}

type CIDLink struct {
	Link string `json:"$link"`
}

type LikeRecord struct {
	Type      string    `json:"$type"`
	Subject   StrongRef `json:"subject"`
	CreatedAt string    `json:"createdAt"`
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

func initDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS posts (
			uri         TEXT PRIMARY KEY,
			cid         TEXT NOT NULL,
			author_did  TEXT NOT NULL,
			blob_cid    TEXT NOT NULL,
			mime_type   TEXT NOT NULL,
			duration    INTEGER NOT NULL,
			waveform    TEXT,
			reply_root  TEXT,
			reply_parent TEXT,
			created_at  TEXT NOT NULL,
			indexed_at  TEXT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author_did);
		CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at DESC);

		CREATE TABLE IF NOT EXISTS likes (
			uri         TEXT PRIMARY KEY,
			cid         TEXT NOT NULL,
			author_did  TEXT NOT NULL,
			subject_uri TEXT NOT NULL,
			subject_cid TEXT NOT NULL,
			created_at  TEXT NOT NULL,
			indexed_at  TEXT NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_likes_subject ON likes(subject_uri);

		CREATE TABLE IF NOT EXISTS cursor (
			id     INTEGER PRIMARY KEY CHECK (id = 1),
			time_us INTEGER NOT NULL
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("creating tables: %w", err)
	}

	return db, nil
}

// ---------------------------------------------------------------------------
// Firehose consumer
// ---------------------------------------------------------------------------

func buildJetstreamURL(cursor int64) string {
	u := url.URL{
		Scheme: "wss",
		Host:   "jetstream2.us-west.bsky.network",
		Path:   "/subscribe",
	}
	q := u.Query()
	q.Set("wantedCollections", "at.wavelength.audio.post")
	q.Add("wantedCollections", "at.wavelength.audio.like")
	if cursor > 0 {
		q.Set("cursor", fmt.Sprintf("%d", cursor))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func getCursor(db *sql.DB) int64 {
	var timeUS int64
	err := db.QueryRow("SELECT time_us FROM cursor WHERE id = 1").Scan(&timeUS)
	if err != nil {
		return 0
	}
	return timeUS
}

func saveCursor(db *sql.DB, timeUS int64) {
	db.Exec(`INSERT INTO cursor (id, time_us) VALUES (1, ?)
		ON CONFLICT(id) DO UPDATE SET time_us = excluded.time_us`, timeUS)
}

func consumeFirehose(ctx context.Context, db *sql.DB, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cursor := getCursor(db)
		wsURL := buildJetstreamURL(cursor)
		log.Info("connecting to jetstream", "url", wsURL)

		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err != nil {
			log.Error("jetstream dial failed", "err", err)
			time.Sleep(3 * time.Second)
			continue
		}

		if err := readLoop(ctx, conn, db, log); err != nil {
			log.Error("jetstream read error", "err", err)
		}
		conn.Close()
		time.Sleep(time.Second)
	}
}

func readLoop(ctx context.Context, conn *websocket.Conn, db *sql.DB, log *slog.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var evt JetstreamEvent
		if err := json.Unmarshal(msg, &evt); err != nil {
			log.Warn("unmarshal failed", "err", err)
			continue
		}

		if evt.Kind != "commit" || evt.Commit == nil {
			continue
		}

		switch evt.Commit.Collection {
		case "at.wavelength.audio.post":
			handlePost(db, log, &evt)
		case "at.wavelength.audio.like":
			handleLike(db, log, &evt)
		}

		saveCursor(db, evt.TimeUS)
	}
}

func handlePost(db *sql.DB, log *slog.Logger, evt *JetstreamEvent) {
	c := evt.Commit
	uri := fmt.Sprintf("at://%s/%s/%s", evt.Did, c.Collection, c.Rkey)

	switch c.Operation {
	case "create", "update":
		var rec AudioPostRecord
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			log.Warn("bad post record", "uri", uri, "err", err)
			return
		}

		waveformJSON, _ := json.Marshal(rec.Waveform)

		var replyRoot, replyParent *string
		if rec.Reply != nil {
			replyRoot = &rec.Reply.Root.URI
			replyParent = &rec.Reply.Parent.URI
		}

		_, err := db.Exec(`
			INSERT INTO posts (uri, cid, author_did, blob_cid, mime_type, duration, waveform, reply_root, reply_parent, created_at, indexed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(uri) DO UPDATE SET
				cid = excluded.cid,
				blob_cid = excluded.blob_cid,
				duration = excluded.duration,
				waveform = excluded.waveform,
				reply_root = excluded.reply_root,
				reply_parent = excluded.reply_parent,
				indexed_at = excluded.indexed_at
		`, uri, c.CID, evt.Did, rec.Audio.Ref.Link, rec.Audio.MimeType, rec.Duration,
			string(waveformJSON), replyRoot, replyParent, rec.CreatedAt, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			log.Error("insert post", "uri", uri, "err", err)
		}

	case "delete":
		db.Exec("DELETE FROM posts WHERE uri = ?", uri)
		db.Exec("DELETE FROM likes WHERE subject_uri = ?", uri)
	}
}

func handleLike(db *sql.DB, log *slog.Logger, evt *JetstreamEvent) {
	c := evt.Commit
	uri := fmt.Sprintf("at://%s/%s/%s", evt.Did, c.Collection, c.Rkey)

	switch c.Operation {
	case "create":
		var rec LikeRecord
		if err := json.Unmarshal(c.Record, &rec); err != nil {
			log.Warn("bad like record", "uri", uri, "err", err)
			return
		}

		_, err := db.Exec(`
			INSERT OR IGNORE INTO likes (uri, cid, author_did, subject_uri, subject_cid, created_at, indexed_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, uri, c.CID, evt.Did, rec.Subject.URI, rec.Subject.CID, rec.CreatedAt, time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			log.Error("insert like", "uri", uri, "err", err)
		}

	case "delete":
		db.Exec("DELETE FROM likes WHERE uri = ?", uri)
	}
}

// ---------------------------------------------------------------------------
// XRPC query endpoints
// ---------------------------------------------------------------------------

type PostViewResponse struct {
	URI        string  `json:"uri"`
	CID        string  `json:"cid"`
	AuthorDID  string  `json:"author_did"`
	BlobCID    string  `json:"blob_cid"`
	MimeType   string  `json:"mime_type"`
	Duration   int     `json:"duration"`
	Waveform   []int   `json:"waveform,omitempty"`
	ReplyRoot  *string `json:"reply_root,omitempty"`
	ReplyParent *string `json:"reply_parent,omitempty"`
	CreatedAt  string  `json:"created_at"`
	LikeCount  int     `json:"like_count"`
	ReplyCount int     `json:"reply_count"`
}

func handleGetFeed(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			fmt.Sscanf(l, "%d", &limit)
			if limit < 1 { limit = 1 }
			if limit > 100 { limit = 100 }
		}
		cursor := r.URL.Query().Get("cursor")

		query := `
			SELECT p.uri, p.cid, p.author_did, p.blob_cid, p.mime_type, p.duration,
				p.waveform, p.reply_root, p.reply_parent, p.created_at,
				(SELECT COUNT(*) FROM likes l WHERE l.subject_uri = p.uri) as like_count,
				(SELECT COUNT(*) FROM posts r WHERE r.reply_parent = p.uri) as reply_count
			FROM posts p
			WHERE p.reply_root IS NULL
		`
		args := []any{}
		if cursor != "" {
			query += " AND p.created_at < ?"
			args = append(args, cursor)
		}
		query += " ORDER BY p.created_at DESC LIMIT ?"
		args = append(args, limit)

		rows, err := db.Query(query, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()

		posts := []PostViewResponse{}
		var lastCreatedAt string
		for rows.Next() {
			var p PostViewResponse
			var waveformJSON sql.NullString
			var replyRoot, replyParent sql.NullString

			err := rows.Scan(&p.URI, &p.CID, &p.AuthorDID, &p.BlobCID, &p.MimeType,
				&p.Duration, &waveformJSON, &replyRoot, &replyParent, &p.CreatedAt,
				&p.LikeCount, &p.ReplyCount)
			if err != nil {
				continue
			}

			if waveformJSON.Valid {
				json.Unmarshal([]byte(waveformJSON.String), &p.Waveform)
			}
			if replyRoot.Valid { p.ReplyRoot = &replyRoot.String }
			if replyParent.Valid { p.ReplyParent = &replyParent.String }

			lastCreatedAt = p.CreatedAt
			posts = append(posts, p)
		}

		resp := map[string]any{"posts": posts}
		if len(posts) == limit {
			resp["cursor"] = lastCreatedAt
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(resp)
	}
}

func handleGetAuthorFeed(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor := r.URL.Query().Get("actor")
		if actor == "" {
			http.Error(w, `{"error":"InvalidRequest","message":"actor is required"}`, 400)
			return
		}

		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			fmt.Sscanf(l, "%d", &limit)
			if limit < 1 { limit = 1 }
			if limit > 100 { limit = 100 }
		}
		cursor := r.URL.Query().Get("cursor")

		// actor can be a DID or handle — for now we only support DID
		// handle resolution would require a DID resolver
		did := actor
		if !strings.HasPrefix(did, "did:") {
			http.Error(w, `{"error":"InvalidRequest","message":"handle resolution not yet supported, use DID"}`, 400)
			return
		}

		query := `
			SELECT p.uri, p.cid, p.author_did, p.blob_cid, p.mime_type, p.duration,
				p.waveform, p.reply_root, p.reply_parent, p.created_at,
				(SELECT COUNT(*) FROM likes l WHERE l.subject_uri = p.uri) as like_count,
				(SELECT COUNT(*) FROM posts r WHERE r.reply_parent = p.uri) as reply_count
			FROM posts p
			WHERE p.author_did = ?
		`
		args := []any{did}
		if cursor != "" {
			query += " AND p.created_at < ?"
			args = append(args, cursor)
		}
		query += " ORDER BY p.created_at DESC LIMIT ?"
		args = append(args, limit)

		rows, err := db.Query(query, args...)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()

		posts := []PostViewResponse{}
		var lastCreatedAt string
		for rows.Next() {
			var p PostViewResponse
			var waveformJSON sql.NullString
			var replyRoot, replyParent sql.NullString

			err := rows.Scan(&p.URI, &p.CID, &p.AuthorDID, &p.BlobCID, &p.MimeType,
				&p.Duration, &waveformJSON, &replyRoot, &replyParent, &p.CreatedAt,
				&p.LikeCount, &p.ReplyCount)
			if err != nil {
				continue
			}

			if waveformJSON.Valid {
				json.Unmarshal([]byte(waveformJSON.String), &p.Waveform)
			}
			if replyRoot.Valid { p.ReplyRoot = &replyRoot.String }
			if replyParent.Valid { p.ReplyParent = &replyParent.String }

			lastCreatedAt = p.CreatedAt
			posts = append(posts, p)
		}

		resp := map[string]any{"posts": posts}
		if len(posts) == limit {
			resp["cursor"] = lastCreatedAt
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(resp)
	}
}

func handleGetPostThread(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uri := r.URL.Query().Get("uri")
		if uri == "" {
			http.Error(w, `{"error":"InvalidRequest","message":"uri is required"}`, 400)
			return
		}

		// Fetch the post
		var p PostViewResponse
		var waveformJSON sql.NullString
		var replyRoot, replyParent sql.NullString

		err := db.QueryRow(`
			SELECT p.uri, p.cid, p.author_did, p.blob_cid, p.mime_type, p.duration,
				p.waveform, p.reply_root, p.reply_parent, p.created_at,
				(SELECT COUNT(*) FROM likes l WHERE l.subject_uri = p.uri) as like_count,
				(SELECT COUNT(*) FROM posts r WHERE r.reply_parent = p.uri) as reply_count
			FROM posts p WHERE p.uri = ?
		`, uri).Scan(&p.URI, &p.CID, &p.AuthorDID, &p.BlobCID, &p.MimeType,
			&p.Duration, &waveformJSON, &replyRoot, &replyParent, &p.CreatedAt,
			&p.LikeCount, &p.ReplyCount)

		if err == sql.ErrNoRows {
			http.Error(w, `{"error":"NotFound","message":"post not found"}`, 404)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		if waveformJSON.Valid {
			json.Unmarshal([]byte(waveformJSON.String), &p.Waveform)
		}
		if replyRoot.Valid { p.ReplyRoot = &replyRoot.String }
		if replyParent.Valid { p.ReplyParent = &replyParent.String }

		// Fetch direct replies
		replyRows, err := db.Query(`
			SELECT p.uri, p.cid, p.author_did, p.blob_cid, p.mime_type, p.duration,
				p.waveform, p.reply_root, p.reply_parent, p.created_at,
				(SELECT COUNT(*) FROM likes l WHERE l.subject_uri = p.uri) as like_count,
				(SELECT COUNT(*) FROM posts r WHERE r.reply_parent = p.uri) as reply_count
			FROM posts p WHERE p.reply_parent = ?
			ORDER BY p.created_at ASC
		`, uri)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer replyRows.Close()

		replies := []PostViewResponse{}
		for replyRows.Next() {
			var rp PostViewResponse
			var rwf sql.NullString
			var rr, rrp sql.NullString

			if err := replyRows.Scan(&rp.URI, &rp.CID, &rp.AuthorDID, &rp.BlobCID, &rp.MimeType,
				&rp.Duration, &rwf, &rr, &rrp, &rp.CreatedAt, &rp.LikeCount, &rp.ReplyCount); err != nil {
				continue
			}
			if rwf.Valid { json.Unmarshal([]byte(rwf.String), &rp.Waveform) }
			if rr.Valid { rp.ReplyRoot = &rr.String }
			if rrp.Valid { rp.ReplyParent = &rrp.String }
			replies = append(replies, rp)
		}

		resp := map[string]any{
			"thread": map[string]any{
				"post":    p,
				"replies": replies,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(resp)
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := os.Getenv("DATABASE_PATH")
	if dbPath == "" {
		dbPath = "wavelength.db"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "4000"
	}

	db, err := initDB(dbPath)
	if err != nil {
		log.Error("failed to init db", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Start firehose consumer
	go consumeFirehose(ctx, db, log)

	// XRPC endpoints
	mux := http.NewServeMux()
	mux.HandleFunc("GET /xrpc/at.wavelength.audio.getFeed", handleGetFeed(db))
	mux.HandleFunc("GET /xrpc/at.wavelength.audio.getAuthorFeed", handleGetAuthorFeed(db))
	mux.HandleFunc("GET /xrpc/at.wavelength.audio.getPostThread", handleGetPostThread(db))

	// Health check
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	server := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		log.Info("xrpc server starting", "port", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Error("server error", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	server.Shutdown(context.Background())
}

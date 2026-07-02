package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

const serviceName = "post-service"

var (
	httpRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "http_requests_total", Help: "Total HTTP requests"},
		[]string{"service", "method", "path", "status"},
	)
	httpErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "http_errors_total", Help: "Total HTTP errors"},
		[]string{"service", "method", "path", "status"},
	)
	httpLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request duration"},
		[]string{"service", "method", "path"},
	)
	dbLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "db_query_duration_seconds", Help: "Database query duration"},
		[]string{"service", "operation"},
	)
	dbErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "db_errors_total", Help: "Database errors"},
		[]string{"service", "operation"},
	)
	kafkaPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "kafka_messages_published_total", Help: "Kafka messages published"},
		[]string{"service", "topic", "status"},
	)
)

type App struct {
	db      *pgxpool.Pool
	writer  *kafka.Writer
	brokers []string
	topic   string
}

type createPostRequest struct {
	AuthorID string `json:"author_id"`
	Content  string `json:"content"`
}

type postResponse struct {
	ID        string    `json:"id"`
	AuthorID  string    `json:"author_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type eventEnvelope struct {
	EventID      string      `json:"event_id"`
	EventType    string      `json:"event_type"`
	EventVersion int         `json:"event_version"`
	OccurredAt   time.Time   `json:"occurred_at"`
	Producer     string      `json:"producer"`
	TraceID      string      `json:"trace_id"`
	Payload      interface{} `json:"payload"`
}

type postCreatedPayload struct {
	PostID    string    `json:"post_id"`
	AuthorID  string    `json:"author_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	port := env("PORT", "8002")
	databaseURL := env("DATABASE_URL", "postgres://app:app@postgres:5432/social?sslmode=disable")
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := env("POST_CREATED_TOPIC", "post.created.v1")

	ctx := context.Background()
	db := mustConnectPostgres(ctx, databaseURL)
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		Balancer:     &kafka.Hash{},
	}
	defer writer.Close()
	defer db.Close()

	app := &App{db: db, writer: writer, brokers: brokers, topic: topic}
	go app.outboxWorker(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", app.healthz)
	mux.HandleFunc("/readyz", app.readyz)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/posts", app.posts)
	mux.HandleFunc("/posts/", app.postByID)
	mux.HandleFunc("/users/", app.userPosts)

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           metricsMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("%s listening on :%s", serviceName, port)
	log.Fatal(server.ListenAndServe())
}

func (a *App) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": serviceName})
}

func (a *App) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.db.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	conn, err := kafka.DialContext(ctx, "tcp", a.brokers[0])
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	_ = conn.Close()
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": serviceName})
}

func (a *App) posts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var body createPostRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	body.AuthorID = strings.TrimSpace(body.AuthorID)
	body.Content = strings.TrimSpace(body.Content)
	if body.AuthorID == "" || body.Content == "" {
		writeError(w, http.StatusBadRequest, errors.New("author_id and content are required"))
		return
	}
	if len(body.Content) > 500 {
		writeError(w, http.StatusBadRequest, errors.New("content must be 500 characters or less"))
		return
	}

	postID := newID("pst")
	var createdAt time.Time
	start := time.Now()
	err := a.db.QueryRow(
		r.Context(),
		`
		INSERT INTO posts (id, author_id, content)
		VALUES ($1, $2, $3)
		RETURNING created_at
		`,
		postID,
		body.AuthorID,
		body.Content,
	).Scan(&createdAt)
	dbLatency.WithLabelValues(serviceName, "create_post").Observe(time.Since(start).Seconds())
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "create_post").Inc()
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	payload := postCreatedPayload{
		PostID:    postID,
		AuthorID:  body.AuthorID,
		Content:   body.Content,
		CreatedAt: createdAt,
	}
	event := eventEnvelope{
		EventID:      newID("evt"),
		EventType:    "post.created",
		EventVersion: 1,
		OccurredAt:   time.Now().UTC(),
		Producer:     serviceName,
		TraceID:      newID("trc"),
		Payload:      payload,
	}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := a.publish(r.Context(), body.AuthorID, eventBytes); err != nil {
		kafkaPublished.WithLabelValues(serviceName, a.topic, "error").Inc()
		if saveErr := a.saveOutbox(r.Context(), newID("out"), body.AuthorID, eventBytes); saveErr != nil {
			writeError(w, http.StatusInternalServerError, fmt.Errorf("publish failed: %w; outbox failed: %w", err, saveErr))
			return
		}
	} else {
		kafkaPublished.WithLabelValues(serviceName, a.topic, "success").Inc()
	}

	writeJSON(w, http.StatusCreated, postResponse{
		ID:        postID,
		AuthorID:  body.AuthorID,
		Content:   body.Content,
		CreatedAt: createdAt,
	})
}

func (a *App) postByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	postID := strings.TrimPrefix(r.URL.Path, "/posts/")
	var post postResponse
	start := time.Now()
	err := a.db.QueryRow(
		r.Context(),
		"SELECT id, author_id, content, created_at FROM posts WHERE id = $1",
		postID,
	).Scan(&post.ID, &post.AuthorID, &post.Content, &post.CreatedAt)
	dbLatency.WithLabelValues(serviceName, "get_post").Observe(time.Since(start).Seconds())
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "get_post").Inc()
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, post)
}

func (a *App) userPosts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/posts") {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	userID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/users/"), "/posts")
	limit := queryLimit(r, 50, 200)
	start := time.Now()
	rows, err := a.db.Query(
		r.Context(),
		`
		SELECT id, author_id, content, created_at
		FROM posts
		WHERE author_id = $1
		ORDER BY created_at DESC
		LIMIT $2
		`,
		userID,
		limit,
	)
	dbLatency.WithLabelValues(serviceName, "list_user_posts").Observe(time.Since(start).Seconds())
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "list_user_posts").Inc()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer rows.Close()
	posts := []postResponse{}
	for rows.Next() {
		var post postResponse
		if err := rows.Scan(&post.ID, &post.AuthorID, &post.Content, &post.CreatedAt); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		posts = append(posts, post)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"user_id": userID, "posts": posts})
}

func (a *App) publish(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return a.writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value})
}

func (a *App) saveOutbox(ctx context.Context, outboxID, key string, payload []byte) error {
	start := time.Now()
	_, err := a.db.Exec(
		ctx,
		`
		INSERT INTO outbox_events (id, topic, event_key, payload)
		VALUES ($1, $2, $3, $4::jsonb)
		`,
		outboxID,
		a.topic,
		key,
		string(payload),
	)
	dbLatency.WithLabelValues(serviceName, "save_outbox").Observe(time.Since(start).Seconds())
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "save_outbox").Inc()
	}
	return err
}

func (a *App) outboxWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.flushOutbox(ctx)
		}
	}
}

func (a *App) flushOutbox(ctx context.Context) {
	rows, err := a.db.Query(
		ctx,
		`
		SELECT id, topic, event_key, payload::text, attempts
		FROM outbox_events
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT 20
		`,
	)
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "outbox_fetch").Inc()
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id, topic, key, payload string
		var attempts int
		if err := rows.Scan(&id, &topic, &key, &payload, &attempts); err != nil {
			continue
		}
		err := a.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(payload)})
		if err == nil {
			_, _ = a.db.Exec(ctx, "UPDATE outbox_events SET status = 'sent', updated_at = now() WHERE id = $1", id)
			kafkaPublished.WithLabelValues(serviceName, topic, "outbox_success").Inc()
			continue
		}
		nextStatus := "pending"
		if attempts >= 5 {
			nextStatus = "failed"
		}
		_, _ = a.db.Exec(
			ctx,
			"UPDATE outbox_events SET attempts = attempts + 1, status = $2, updated_at = now() WHERE id = $1",
			id,
			nextStatus,
		)
		kafkaPublished.WithLabelValues(serviceName, topic, "outbox_error").Inc()
	}
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		path := routePattern(r)
		start := time.Now()
		next.ServeHTTP(rec, r)
		status := strconv.Itoa(rec.status)
		httpRequests.WithLabelValues(serviceName, r.Method, path, status).Inc()
		httpLatency.WithLabelValues(serviceName, r.Method, path).Observe(time.Since(start).Seconds())
		if rec.status >= 400 {
			httpErrors.WithLabelValues(serviceName, r.Method, path, status).Inc()
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func routePattern(r *http.Request) string {
	path := r.URL.Path
	if strings.HasPrefix(path, "/posts/") {
		return "/posts/{post_id}"
	}
	if strings.HasPrefix(path, "/users/") && strings.HasSuffix(path, "/posts") {
		return "/users/{user_id}/posts"
	}
	return path
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]interface{}{
		"error": map[string]string{
			"code":    http.StatusText(status),
			"message": err.Error(),
		},
	})
}

func queryLimit(r *http.Request, fallback, max int) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	if value > max {
		return max
	}
	return value
}

func mustConnectPostgres(ctx context.Context, databaseURL string) *pgxpool.Pool {
	var lastErr error
	for i := 0; i < 30; i++ {
		pool, err := pgxpool.New(ctx, databaseURL)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				return pool
			}
			pool.Close()
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("postgres unavailable: %v", lastErr)
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}


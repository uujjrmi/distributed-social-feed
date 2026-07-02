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
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const serviceName = "feed-service"

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
	redisOps = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "redis_operations_total", Help: "Redis operations"},
		[]string{"service", "operation", "status"},
	)
	redisDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{Name: "redis_operation_duration_seconds", Help: "Redis operation duration"},
		[]string{"service", "operation"},
	)
	redisErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "redis_errors_total", Help: "Redis errors"},
		[]string{"service", "operation"},
	)
	cacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "feed_cache_hits_total", Help: "Feed cache hits"},
		[]string{"service"},
	)
	cacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "feed_cache_misses_total", Help: "Feed cache misses"},
		[]string{"service"},
	)
	degradedRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "degraded_mode_requests_total", Help: "Requests served in degraded mode"},
		[]string{"service"},
	)
	kafkaConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{Name: "kafka_messages_consumed_total", Help: "Kafka messages consumed"},
		[]string{"service", "topic", "status"},
	)
	kafkaLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{Name: "kafka_consumer_lag", Help: "Kafka consumer lag"},
		[]string{"service", "topic", "partition"},
	)
)

type App struct {
	db             *pgxpool.Pool
	redis          *redis.Client
	reader         *kafka.Reader
	brokers        []string
	topic          string
	group          string
	adminToken     string
	feedMaxItems   int
	degradedMode   atomic.Bool
	consumerPaused atomic.Bool
}

type eventEnvelope struct {
	EventID   string          `json:"event_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type postCreatedPayload struct {
	PostID    string    `json:"post_id"`
	AuthorID  string    `json:"author_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type feedItem struct {
	PostID    string    `json:"post_id"`
	AuthorID  string    `json:"author_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	port := env("PORT", "8003")
	databaseURL := env("DATABASE_URL", "postgres://app:app@postgres:5432/social?sslmode=disable")
	redisAddr := env("REDIS_ADDR", "redis:6379")
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := env("POST_CREATED_TOPIC", "post.created.v1")
	group := env("CONSUMER_GROUP", "feed-service")
	adminToken := env("ADMIN_TOKEN", "dev-admin-token")
	feedMaxItems := envInt("FEED_MAX_ITEMS", 500)

	ctx := context.Background()
	db := mustConnectPostgres(ctx, databaseURL)
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer db.Close()
	defer rdb.Close()
	defer reader.Close()

	app := &App{
		db:           db,
		redis:        rdb,
		reader:       reader,
		brokers:      brokers,
		topic:        topic,
		group:        group,
		adminToken:   adminToken,
		feedMaxItems: feedMaxItems,
	}
	go app.consumeForever(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", app.healthz)
	mux.HandleFunc("/readyz", app.readyz)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/feeds/", app.getFeed)
	mux.HandleFunc("/admin/degraded-mode", app.setDegradedMode)
	mux.HandleFunc("/admin/consumer-pause", app.setConsumerPaused)

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
	kafkaStatus := "ready"
	conn, err := kafka.DialContext(ctx, "tcp", a.brokers[0])
	if err != nil {
		kafkaStatus = "unavailable"
	} else {
		_ = conn.Close()
	}
	redisStatus := "ready"
	if err := a.redis.Ping(ctx).Err(); err != nil {
		redisStatus = "degraded"
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ready", "service": serviceName, "redis": redisStatus, "kafka": kafkaStatus,
	})
}

func (a *App) getFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	userID := strings.TrimPrefix(r.URL.Path, "/feeds/")
	limit := queryLimit(r, 50, 200)
	if userID == "" {
		writeError(w, http.StatusBadRequest, errors.New("user id is required"))
		return
	}

	if a.degradedMode.Load() {
		items, err := a.feedFromPostgres(r.Context(), userID, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		degradedRequests.WithLabelValues(serviceName).Inc()
		writeJSON(w, http.StatusOK, map[string]interface{}{"user_id": userID, "source": "postgres_degraded", "items": items})
		return
	}

	items, err := a.feedFromRedis(r.Context(), userID, limit)
	if err == nil {
		cacheHits.WithLabelValues(serviceName).Inc()
		writeJSON(w, http.StatusOK, map[string]interface{}{"user_id": userID, "source": "redis", "items": items})
		return
	}

	cacheMisses.WithLabelValues(serviceName).Inc()
	items, fallbackErr := a.feedFromPostgres(r.Context(), userID, limit)
	if fallbackErr != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("redis failed: %w; postgres fallback failed: %w", err, fallbackErr))
		return
	}
	degradedRequests.WithLabelValues(serviceName).Inc()
	writeJSON(w, http.StatusOK, map[string]interface{}{"user_id": userID, "source": "postgres_degraded", "items": items})
}

func (a *App) setDegradedMode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !a.authorized(r) {
		writeError(w, http.StatusUnauthorized, errors.New("invalid admin token"))
		return
	}
	var body struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	a.degradedMode.Store(body.Enabled)
	writeJSON(w, http.StatusOK, map[string]interface{}{"service": serviceName, "degraded_mode": body.Enabled})
}

func (a *App) setConsumerPaused(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !a.authorized(r) {
		writeError(w, http.StatusUnauthorized, errors.New("invalid admin token"))
		return
	}
	var body struct {
		Paused bool `json:"paused"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	a.consumerPaused.Store(body.Paused)
	writeJSON(w, http.StatusOK, map[string]interface{}{"service": serviceName, "consumer_paused": body.Paused})
}

func (a *App) feedFromRedis(ctx context.Context, userID string, limit int) ([]feedItem, error) {
	start := time.Now()
	postIDs, err := a.redis.ZRevRange(ctx, feedKey(userID), 0, int64(limit-1)).Result()
	redisDuration.WithLabelValues(serviceName, "zrevrange_feed").Observe(time.Since(start).Seconds())
	if err != nil {
		redisErrors.WithLabelValues(serviceName, "zrevrange_feed").Inc()
		redisOps.WithLabelValues(serviceName, "zrevrange_feed", "error").Inc()
		return nil, err
	}
	redisOps.WithLabelValues(serviceName, "zrevrange_feed", "success").Inc()
	if len(postIDs) == 0 {
		return nil, errors.New("feed cache empty")
	}

	items := make([]feedItem, 0, len(postIDs))
	for _, postID := range postIDs {
		start = time.Now()
		data, err := a.redis.HGetAll(ctx, postSnapshotKey(postID)).Result()
		redisDuration.WithLabelValues(serviceName, "hgetall_post_snapshot").Observe(time.Since(start).Seconds())
		if err != nil {
			redisErrors.WithLabelValues(serviceName, "hgetall_post_snapshot").Inc()
			redisOps.WithLabelValues(serviceName, "hgetall_post_snapshot", "error").Inc()
			return nil, err
		}
		redisOps.WithLabelValues(serviceName, "hgetall_post_snapshot", "success").Inc()
		if len(data) == 0 {
			return nil, errors.New("post snapshot missing")
		}
		createdAt, _ := time.Parse(time.RFC3339Nano, data["created_at"])
		items = append(items, feedItem{
			PostID:    postID,
			AuthorID:  data["author_id"],
			Content:   data["content"],
			CreatedAt: createdAt,
		})
	}
	return items, nil
}

func (a *App) feedFromPostgres(ctx context.Context, userID string, limit int) ([]feedItem, error) {
	start := time.Now()
	rows, err := a.db.Query(
		ctx,
		`
		SELECT p.id, p.author_id, p.content, p.created_at
		FROM follows f
		JOIN posts p ON p.author_id = f.followee_id
		WHERE f.follower_id = $1
		ORDER BY p.created_at DESC
		LIMIT $2
		`,
		userID,
		limit,
	)
	dbLatency.WithLabelValues(serviceName, "feed_from_postgres").Observe(time.Since(start).Seconds())
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "feed_from_postgres").Inc()
		return nil, err
	}
	defer rows.Close()

	items := []feedItem{}
	for rows.Next() {
		var item feedItem
		if err := rows.Scan(&item.PostID, &item.AuthorID, &item.Content, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (a *App) consumeForever(ctx context.Context) {
	for {
		if a.consumerPaused.Load() {
			kafkaLag.WithLabelValues(serviceName, a.topic, "all").Set(1001)
			time.Sleep(2 * time.Second)
			continue
		}
		kafkaLag.WithLabelValues(serviceName, a.topic, "all").Set(0)
		msg, err := a.reader.FetchMessage(ctx)
		if err != nil {
			kafkaConsumed.WithLabelValues(serviceName, a.topic, "fetch_error").Inc()
			time.Sleep(2 * time.Second)
			continue
		}
		if err := a.processMessage(ctx, msg.Value); err != nil {
			kafkaConsumed.WithLabelValues(serviceName, a.topic, "error").Inc()
			log.Printf("failed processing kafka message: %v", err)
			continue
		}
		if err := a.reader.CommitMessages(ctx, msg); err != nil {
			kafkaConsumed.WithLabelValues(serviceName, a.topic, "commit_error").Inc()
			continue
		}
		kafkaConsumed.WithLabelValues(serviceName, a.topic, "success").Inc()
	}
}

func (a *App) processMessage(ctx context.Context, value []byte) error {
	var event eventEnvelope
	if err := json.Unmarshal(value, &event); err != nil {
		return err
	}
	if event.EventType != "post.created" {
		return nil
	}
	var payload postCreatedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}

	tx, err := a.db.Begin(ctx)
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "begin_event").Inc()
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(
		ctx,
		`
		INSERT INTO processed_events (consumer_name, event_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING
		`,
		a.group,
		event.EventID,
	)
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "insert_processed_event").Inc()
		return err
	}
	if tag.RowsAffected() == 0 {
		kafkaConsumed.WithLabelValues(serviceName, a.topic, "duplicate").Inc()
		return nil
	}

	rows, err := tx.Query(ctx, "SELECT follower_id FROM follows WHERE followee_id = $1", payload.AuthorID)
	if err != nil {
		dbErrors.WithLabelValues(serviceName, "followers_for_post").Inc()
		return err
	}
	followers := []string{}
	for rows.Next() {
		var followerID string
		if err := rows.Scan(&followerID); err != nil {
			rows.Close()
			return err
		}
		followers = append(followers, followerID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	start := time.Now()
	pipe := a.redis.Pipeline()
	pipe.HSet(ctx, postSnapshotKey(payload.PostID), map[string]interface{}{
		"author_id":  payload.AuthorID,
		"content":    payload.Content,
		"created_at": payload.CreatedAt.Format(time.RFC3339Nano),
	})
	pipe.Expire(ctx, postSnapshotKey(payload.PostID), 24*time.Hour)
	for _, followerID := range followers {
		key := feedKey(followerID)
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(payload.CreatedAt.UnixMilli()), Member: payload.PostID})
		pipe.ZRemRangeByRank(ctx, key, 0, int64(-a.feedMaxItems-1))
	}
	_, err = pipe.Exec(ctx)
	redisDuration.WithLabelValues(serviceName, "materialize_feed").Observe(time.Since(start).Seconds())
	if err != nil {
		redisErrors.WithLabelValues(serviceName, "materialize_feed").Inc()
		redisOps.WithLabelValues(serviceName, "materialize_feed", "error").Inc()
		return err
	}
	redisOps.WithLabelValues(serviceName, "materialize_feed", "success").Inc()

	return tx.Commit(ctx)
}

func (a *App) authorized(r *http.Request) bool {
	return r.Header.Get("x-admin-token") == a.adminToken
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
	if strings.HasPrefix(path, "/feeds/") {
		return "/feeds/{user_id}"
	}
	if strings.HasPrefix(path, "/admin/") {
		return path
	}
	return path
}

func feedKey(userID string) string {
	return "feed:" + userID
}

func postSnapshotKey(postID string) string {
	return "post_snapshot:" + postID
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

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}


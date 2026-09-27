package inframetrics

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "http_requests_total",
			Help:      "Total HTTP requests handled by the API.",
		},
		[]string{"method", "route", "status"},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"method", "route", "status"},
	)

	FeedRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_requests_total",
			Help:      "Total feed requests by scene and result.",
		},
		[]string{"scene", "result"},
	)

	FeedRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "feed_request_duration_seconds",
			Help:      "Feed request duration in seconds by scene.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"scene", "result"},
	)

	FeedItemsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_items_total",
			Help:      "Total feed items returned by scene.",
		},
		[]string{"scene"},
	)

	FeedEmptyRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_empty_requests_total",
			Help:      "Successful feed requests that returned no items.",
		},
		[]string{"scene"},
	)

	FeedCandidateItemsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_candidate_items_total",
			Help:      "Valid feed candidates entering deduplication.",
		},
		[]string{"scene"},
	)

	FeedDuplicateItemsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_duplicate_items_total",
			Help:      "Duplicate feed candidates removed by deduplication.",
		},
		[]string{"scene"},
	)

	FeedRankLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "feed_rank_latency_seconds",
			Help:      "Feed ranking latency in seconds by scene.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
		},
		[]string{"scene"},
	)

	FeedFanoutLatency = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "feed_fanout_latency_seconds",
			Help:      "Feed fanout processing latency in seconds.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	)

	FeedFanoutFailureTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_fanout_failure_total",
			Help:      "Feed fanout operations that failed.",
		},
	)

	FeedCacheRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_cache_requests_total",
			Help:      "Feed cache reads by cache area and result.",
		},
		[]string{"area", "result"},
	)

	FeedCacheWritesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "feed_cache_writes_total",
			Help:      "Feed cache writes by cache area and result.",
		},
		[]string{"area", "result"},
	)

	EventBusEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "event_bus_events_total",
			Help:      "Event bus operations by backend, event type and result.",
		},
		[]string{"backend", "event_type", "operation", "result"},
	)

	KafkaConsumerRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "kafka_consumer_retries_total",
			Help:      "Kafka consumer handler retries by group and event type.",
		},
		[]string{"group", "event_type"},
	)

	KafkaDLQEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "kafka_dlq_events_total",
			Help:      "Events sent to Kafka dead-letter topics by group and event type.",
		},
		[]string{"group", "event_type", "result"},
	)

	KafkaConsumerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "kafka_consumer_processing_duration_seconds",
			Help:      "Kafka event processing duration including retries.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
		[]string{"group", "event_type", "result"},
	)

	KafkaConsumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gcfeed",
			Name:      "kafka_consumer_lag",
			Help:      "Kafka consumer lag by group, topic and partition.",
		},
		[]string{"group", "topic", "partition"},
	)

	OutboxEvents = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gcfeed",
			Name:      "outbox_events",
			Help:      "Current outbox event count by status.",
		},
		[]string{"status"},
	)

	OutboxOldestPendingAge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "gcfeed",
			Name:      "outbox_oldest_pending_age_seconds",
			Help:      "Age of the oldest pending outbox event in seconds.",
		},
	)

	RecommendationRecallCandidates = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "recommendation_recall_candidate_count",
			Help:      "Candidate count returned by each recommendation recall source.",
			Buckets:   []float64{0, 1, 5, 10, 20, 50, 100, 200, 500},
		},
		[]string{"source", "result"},
	)

	RecommendationRecallDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "recommendation_recall_duration_seconds",
			Help:      "Recommendation recall latency by source.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"source", "result"},
	)

	VideoUploadTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "video_upload_total",
			Help:      "Upload requests by kind and result.",
		},
		[]string{"kind", "result"},
	)

	VideoUploadDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "video_upload_duration_seconds",
			Help:      "Upload request processing duration in seconds.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
		},
		[]string{"kind", "result"},
	)

	VideoProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "video_processing_duration_seconds",
			Help:      "Video processing step duration in seconds.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
		},
		[]string{"step", "result"},
	)

	WorkerJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "worker_jobs_total",
			Help:      "Worker jobs handled by job name and result.",
		},
		[]string{"job", "result"},
	)

	WorkerJobDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gcfeed",
			Name:      "worker_job_duration_seconds",
			Help:      "Worker job processing duration in seconds.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"job", "result"},
	)

	GovernanceEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gcfeed",
			Name:      "governance_events_total",
			Help:      "System governance decisions by component and outcome.",
		},
		[]string{"component", "decision"},
	)
)

func init() {
	prometheus.MustRegister(
		HTTPRequestsTotal,
		HTTPRequestDuration,
		FeedRequestsTotal,
		FeedRequestDuration,
		FeedItemsTotal,
		FeedEmptyRequestsTotal,
		FeedCandidateItemsTotal,
		FeedDuplicateItemsTotal,
		FeedRankLatency,
		FeedFanoutLatency,
		FeedFanoutFailureTotal,
		FeedCacheRequestsTotal,
		FeedCacheWritesTotal,
		EventBusEventsTotal,
		KafkaConsumerRetriesTotal,
		KafkaDLQEventsTotal,
		KafkaConsumerDuration,
		KafkaConsumerLag,
		OutboxEvents,
		OutboxOldestPendingAge,
		RecommendationRecallCandidates,
		RecommendationRecallDuration,
		VideoUploadTotal,
		VideoUploadDuration,
		VideoProcessingDuration,
		WorkerJobsTotal,
		WorkerJobDuration,
		GovernanceEventsTotal,
	)
}

func ObserveGovernance(component string, decision string) {
	GovernanceEventsTotal.WithLabelValues(
		normalizeLabel(component, "unknown"),
		normalizeLabel(decision, "unknown"),
	).Inc()
}

func ObserveRecommendationRecall(source string, count int, duration time.Duration, err error) {
	source = normalizeLabel(source, "unknown")
	result := resultLabel(err)
	RecommendationRecallCandidates.WithLabelValues(source, result).Observe(float64(count))
	RecommendationRecallDuration.WithLabelValues(source, result).Observe(duration.Seconds())
}

func SetOutboxStatus(pending int64, failed int64, oldestPendingAt *time.Time) {
	OutboxEvents.WithLabelValues("pending").Set(float64(pending))
	OutboxEvents.WithLabelValues("failed").Set(float64(failed))
	age := 0.0
	if oldestPendingAt != nil {
		age = time.Since(*oldestPendingAt).Seconds()
		if age < 0 {
			age = 0
		}
	}
	OutboxOldestPendingAge.Set(age)
}

func ObserveKafkaRetry(group string, eventType string) {
	KafkaConsumerRetriesTotal.WithLabelValues(normalizeLabel(group, "unknown"), normalizeLabel(eventType, "unknown")).Inc()
}

func ObserveKafkaDLQ(group string, eventType string, err error) {
	KafkaDLQEventsTotal.WithLabelValues(normalizeLabel(group, "unknown"), normalizeLabel(eventType, "unknown"), resultLabel(err)).Inc()
}

func ObserveKafkaConsumer(group string, eventType string, duration time.Duration, err error) {
	KafkaConsumerDuration.WithLabelValues(normalizeLabel(group, "unknown"), normalizeLabel(eventType, "unknown"), resultLabel(err)).Observe(duration.Seconds())
}

func ObserveKafkaConsumerLag(group string, topic string, partition int32, lag int64) {
	if lag < 0 {
		lag = 0
	}
	KafkaConsumerLag.WithLabelValues(
		normalizeLabel(group, "unknown"),
		normalizeLabel(topic, "unknown"),
		strconv.FormatInt(int64(partition), 10),
	).Set(float64(lag))
}

// RegisterDatabase exposes database/sql pool state without a polling goroutine.
func RegisterDatabase(db *sql.DB) error {
	collectors := []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "gcfeed", Name: "mysql_connections_open", Help: "Current open MySQL connections."}, func() float64 { return float64(db.Stats().OpenConnections) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "gcfeed", Name: "mysql_connections_in_use", Help: "Current MySQL connections in use."}, func() float64 { return float64(db.Stats().InUse) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "gcfeed", Name: "mysql_connections_idle", Help: "Current idle MySQL connections."}, func() float64 { return float64(db.Stats().Idle) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "gcfeed", Name: "mysql_connections_max_open", Help: "Configured maximum open MySQL connections."}, func() float64 { return float64(db.Stats().MaxOpenConnections) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "gcfeed", Name: "mysql_connection_wait_total", Help: "Total waits for a free MySQL connection."}, func() float64 { return float64(db.Stats().WaitCount) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "gcfeed", Name: "mysql_connection_wait_duration_seconds_total", Help: "Total time blocked waiting for a MySQL connection."}, func() float64 { return db.Stats().WaitDuration.Seconds() }),
	}
	for _, collector := range collectors {
		if err := prometheus.Register(collector); err != nil {
			return err
		}
	}
	return nil
}

// HTTPMiddleware records request count and latency with stable route labels.
func HTTPMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path
		}
		status := strconv.Itoa(c.Writer.Status())
		method := c.Request.Method
		duration := time.Since(start).Seconds()

		HTTPRequestsTotal.WithLabelValues(method, route, status).Inc()
		HTTPRequestDuration.WithLabelValues(method, route, status).Observe(duration)
	}
}

func Handler() http.Handler {
	return promhttp.Handler()
}

func RunServer(ctx context.Context, addr string) error {
	server := &http.Server{
		Addr:    addr,
		Handler: Handler(),
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func ObserveFeed(scene string, duration time.Duration, itemCount int, err error) {
	scene = normalizeLabel(scene, "unknown")
	result := resultLabel(err)
	FeedRequestsTotal.WithLabelValues(scene, result).Inc()
	FeedRequestDuration.WithLabelValues(scene, result).Observe(duration.Seconds())
	if err == nil && itemCount > 0 {
		FeedItemsTotal.WithLabelValues(scene).Add(float64(itemCount))
	}
	if err == nil && itemCount == 0 {
		FeedEmptyRequestsTotal.WithLabelValues(scene).Inc()
	}
}

func ObserveFeedDedup(scene string, candidates int, duplicates int) {
	scene = normalizeLabel(scene, "unknown")
	if candidates > 0 {
		FeedCandidateItemsTotal.WithLabelValues(scene).Add(float64(candidates))
	}
	if duplicates > 0 {
		FeedDuplicateItemsTotal.WithLabelValues(scene).Add(float64(duplicates))
	}
}

func ObserveFeedRank(scene string, duration time.Duration) {
	FeedRankLatency.WithLabelValues(normalizeLabel(scene, "unknown")).Observe(duration.Seconds())
}

func ObserveFeedFanout(duration time.Duration, err error) {
	FeedFanoutLatency.Observe(duration.Seconds())
	if err != nil {
		FeedFanoutFailureTotal.Inc()
	}
}

func ObserveCacheRead(area string, requested int, hit int, err error) {
	area = normalizeLabel(area, "unknown")
	if err != nil {
		FeedCacheRequestsTotal.WithLabelValues(area, "error").Inc()
		return
	}
	if hit > 0 {
		FeedCacheRequestsTotal.WithLabelValues(area, "hit").Add(float64(hit))
	}
	miss := requested - hit
	if miss > 0 {
		FeedCacheRequestsTotal.WithLabelValues(area, "miss").Add(float64(miss))
	}
}

func ObserveCacheWrite(area string, count int, err error) {
	area = normalizeLabel(area, "unknown")
	if count <= 0 {
		count = 1
	}
	FeedCacheWritesTotal.WithLabelValues(area, resultLabel(err)).Add(float64(count))
}

func ObserveEventBus(backend string, eventType string, operation string, err error) {
	EventBusEventsTotal.WithLabelValues(
		normalizeLabel(backend, "unknown"),
		normalizeLabel(eventType, "unknown"),
		normalizeLabel(operation, "unknown"),
		resultLabel(err),
	).Inc()
}

func ObserveUpload(kind string, duration time.Duration, err error) {
	kind = normalizeLabel(kind, "unknown")
	result := resultLabel(err)
	VideoUploadTotal.WithLabelValues(kind, result).Inc()
	VideoUploadDuration.WithLabelValues(kind, result).Observe(duration.Seconds())
}

func ObserveVideoProcessing(step string, duration time.Duration, err error) {
	step = normalizeLabel(step, "unknown")
	VideoProcessingDuration.WithLabelValues(step, resultLabel(err)).Observe(duration.Seconds())
}

func ObserveWorkerJob(job string, duration time.Duration, err error) {
	job = normalizeLabel(job, "unknown")
	result := resultLabel(err)
	WorkerJobsTotal.WithLabelValues(job, result).Inc()
	WorkerJobDuration.WithLabelValues(job, result).Observe(duration.Seconds())
}

func resultLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

func normalizeLabel(value string, fallback string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return fallback
	}
	return value
}

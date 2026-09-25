package inframq

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	infraconfig "FluxFeed/internal/infra/config"
	inframetrics "FluxFeed/internal/infra/metrics"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrEmptyKafkaBrokers = errors.New("kafka brokers are empty")

type Kafka struct {
	producer     *kgo.Client
	config       infraconfig.KafkaConfig
	dedup        applicationeventbus.ConsumerDeduplicator
	retryBackoff time.Duration
	mu           sync.Mutex
	consumers    []*kgo.Client
}

type KafkaOption func(*Kafka)

func WithConsumerDeduplicator(dedup applicationeventbus.ConsumerDeduplicator) KafkaOption {
	return func(k *Kafka) { k.dedup = dedup }
}

func NewKafka(cfg infraconfig.KafkaConfig, options ...KafkaOption) (*Kafka, error) {
	cfg = normalizeKafkaConfig(cfg)
	if len(cfg.Brokers) == 0 {
		return nil, ErrEmptyKafkaBrokers
	}
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
	)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := producer.Ping(pingCtx); err != nil {
		producer.Close()
		return nil, err
	}
	retryBackoff, err := time.ParseDuration(cfg.ConsumerRetryBackoff)
	if err != nil || retryBackoff <= 0 {
		retryBackoff = 200 * time.Millisecond
	}
	kafka := &Kafka{producer: producer, config: cfg, retryBackoff: retryBackoff}
	for _, option := range options {
		option(kafka)
	}
	return kafka, nil
}

func (k *Kafka) Publish(ctx context.Context, topic string, event *applicationeventbus.Event) error {
	if event == nil {
		return applicationeventbus.ErrInvalidEvent
	}
	content, err := json.Marshal(event)
	if err == nil {
		err = k.producer.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte(eventPartitionKey(event)),
			Value: content,
			Headers: []kgo.RecordHeader{
				{Key: "event_id", Value: []byte(event.ID)},
				{Key: "event_type", Value: []byte(event.Type)},
			},
			Timestamp: time.UnixMilli(event.Timestamp),
		}).FirstErr()
	}
	inframetrics.ObserveEventBus("kafka", event.Type, "publish", err)
	return err
}

func (k *Kafka) Subscribe(ctx context.Context, topic string, group string, handler func(context.Context, *applicationeventbus.Event) error) error {
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(k.config.Brokers...),
		kgo.ClientID(k.config.ClientID+"-"+group),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return err
	}
	k.mu.Lock()
	k.consumers = append(k.consumers, consumer)
	k.mu.Unlock()
	go k.consume(ctx, consumer, topic, group, handler)
	return nil
}

func (k *Kafka) consume(ctx context.Context, consumer *kgo.Client, topic string, group string, handler func(context.Context, *applicationeventbus.Event) error) {
	for ctx.Err() == nil {
		fetches := consumer.PollRecords(ctx, 1)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			consumer.AllowRebalance()
			return
		}
		for _, fetchErr := range fetches.Errors() {
			inframetrics.ObserveEventBus("kafka", "unknown", "consume", fetchErr.Err)
		}
		for _, record := range fetches.Records() {
			startedAt := time.Now()
			var event applicationeventbus.Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				inframetrics.ObserveEventBus("kafka", "unknown", "decode", err)
				k.publishDLQUntilSuccess(ctx, topic, group, record, "unknown", err, 0)
				_ = consumer.CommitRecords(ctx, record)
				continue
			}
			if strings.TrimSpace(event.ID) == "" || strings.TrimSpace(event.Type) == "" {
				err := applicationeventbus.ErrInvalidEvent
				inframetrics.ObserveEventBus("kafka", "unknown", "decode", err)
				k.publishDLQUntilSuccess(ctx, topic, group, record, "unknown", err, 0)
				_ = consumer.CommitRecords(ctx, record)
				continue
			}
			err := runWithRetry(ctx, k.config.ConsumerMaxRetries, k.retryBackoff, func() error {
				return k.handleEvent(ctx, group, &event, handler)
			}, func() {
				inframetrics.ObserveKafkaRetry(group, event.Type)
			})
			inframetrics.ObserveEventBus("kafka", event.Type, "consume", err)
			inframetrics.ObserveKafkaConsumer(group, event.Type, time.Since(startedAt), err)
			if err != nil {
				k.publishDLQUntilSuccess(ctx, topic, group, record, event.Type, err, k.config.ConsumerMaxRetries)
			}
			if ctx.Err() == nil {
				_ = consumer.CommitRecords(ctx, record)
			}
		}
		consumer.AllowRebalance()
	}
}

func (k *Kafka) handleEvent(ctx context.Context, group string, event *applicationeventbus.Event, handler func(context.Context, *applicationeventbus.Event) error) error {
	if k.dedup != nil {
		processed, err := k.dedup.IsProcessed(ctx, group, event.ID)
		if err != nil || processed {
			return err
		}
	}
	if err := handler(ctx, event); err != nil {
		return err
	}
	if k.dedup != nil {
		return k.dedup.MarkProcessed(ctx, group, event.ID)
	}
	return nil
}

func (k *Kafka) publishDLQUntilSuccess(ctx context.Context, topic string, group string, source *kgo.Record, eventType string, cause error, attempts int) {
	dlqTopic := topic + ".dlq"
	for ctx.Err() == nil {
		record := &kgo.Record{
			Topic: dlqTopic,
			Key:   append([]byte(nil), source.Key...), Value: append([]byte(nil), source.Value...),
			Headers: []kgo.RecordHeader{
				{Key: "original_topic", Value: []byte(topic)},
				{Key: "consumer_group", Value: []byte(group)},
				{Key: "failure", Value: []byte(cause.Error())},
				{Key: "attempts", Value: []byte(strconv.Itoa(attempts + 1))},
			},
			Timestamp: time.Now(),
		}
		err := k.producer.ProduceSync(ctx, record).FirstErr()
		inframetrics.ObserveKafkaDLQ(group, eventType, err)
		if err == nil {
			return
		}
		if !waitForRetry(ctx, k.retryBackoff) {
			return
		}
	}
}

func runWithRetry(ctx context.Context, maxRetries int, base time.Duration, action func() error, onRetry func()) error {
	var err error
	for retry := 0; retry <= maxRetries; retry++ {
		if err = action(); err == nil {
			return nil
		}
		if retry == maxRetries {
			break
		}
		if onRetry != nil {
			onRetry()
		}
		delay := base << retry
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		if !waitForRetry(ctx, delay) {
			return ctx.Err()
		}
	}
	return err
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (k *Kafka) Close() error {
	if k == nil {
		return nil
	}
	k.mu.Lock()
	consumers := append([]*kgo.Client(nil), k.consumers...)
	k.consumers = nil
	k.mu.Unlock()
	for _, consumer := range consumers {
		consumer.Close()
	}
	if k.producer != nil {
		k.producer.Close()
	}
	return nil
}

func eventPartitionKey(event *applicationeventbus.Event) string {
	if event.VideoID > 0 {
		return strconv.FormatInt(event.VideoID, 10)
	}
	if event.UserID > 0 {
		return strconv.FormatInt(event.UserID, 10)
	}
	return event.ID
}

func normalizeKafkaConfig(cfg infraconfig.KafkaConfig) infraconfig.KafkaConfig {
	brokers := make([]string, 0, len(cfg.Brokers))
	for _, broker := range cfg.Brokers {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	cfg.Brokers = brokers
	setDefault := func(value *string, fallback string) {
		*value = strings.TrimSpace(*value)
		if *value == "" {
			*value = fallback
		}
	}
	setDefault(&cfg.ClientID, "fluxfeed")
	setDefault(&cfg.VideoTopic, "fluxfeed.video")
	setDefault(&cfg.InteractionTopic, "fluxfeed.interaction")
	setDefault(&cfg.ExposureTopic, "fluxfeed.exposure")
	setDefault(&cfg.RelationTopic, "fluxfeed.relation")
	setDefault(&cfg.FanoutGroup, "fluxfeed-fanout")
	setDefault(&cfg.EmbeddingGroup, "fluxfeed-embedding")
	setDefault(&cfg.CounterGroup, "fluxfeed-counter")
	setDefault(&cfg.FeatureGroup, "fluxfeed-feature")
	setDefault(&cfg.RecommendationGroup, "fluxfeed-recommendation")
	if cfg.ConsumerMaxRetries <= 0 {
		cfg.ConsumerMaxRetries = 5
	}
	if cfg.ConsumerRetryBackoff == "" {
		cfg.ConsumerRetryBackoff = "200ms"
	}
	return cfg
}

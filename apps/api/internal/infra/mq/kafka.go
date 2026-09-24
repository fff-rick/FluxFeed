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
	producer  *kgo.Client
	config    infraconfig.KafkaConfig
	mu        sync.Mutex
	consumers []*kgo.Client
}

func NewKafka(cfg infraconfig.KafkaConfig) (*Kafka, error) {
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
	return &Kafka{producer: producer, config: cfg}, nil
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
	go k.consume(ctx, consumer, handler)
	return nil
}

func (k *Kafka) consume(ctx context.Context, consumer *kgo.Client, handler func(context.Context, *applicationeventbus.Event) error) {
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
			var event applicationeventbus.Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				inframetrics.ObserveEventBus("kafka", "unknown", "decode", err)
				_ = consumer.CommitRecords(ctx, record)
				continue
			}
			for {
				err := handler(ctx, &event)
				inframetrics.ObserveEventBus("kafka", event.Type, "consume", err)
				if err == nil {
					_ = consumer.CommitRecords(ctx, record)
					break
				}
				// ponytail: 固定退避并阻塞当前分区；Stage 6 引入指数退避和 DLQ。
				select {
				case <-ctx.Done():
					consumer.AllowRebalance()
					return
				case <-time.After(time.Second):
				}
			}
		}
		consumer.AllowRebalance()
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
	return cfg
}

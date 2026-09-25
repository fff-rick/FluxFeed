package inframq

import (
	applicationvideo "FluxFeed/internal/application/video"
	infraconfig "FluxFeed/internal/infra/config"
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestKafkaVideoEventFanout(t *testing.T) {
	rawBrokers := strings.TrimSpace(os.Getenv("KAFKA_BROKERS"))
	if rawBrokers == "" {
		t.Skip("set KAFKA_BROKERS to run the Kafka integration test")
	}

	suffix := time.Now().UTC().Format("20060102-150405.000000000")
	cfg := infraconfig.KafkaConfig{
		Brokers:        strings.Split(rawBrokers, ","),
		ClientID:       "fluxfeed-test-" + suffix,
		VideoTopic:     "fluxfeed.test.video." + suffix,
		FanoutGroup:    "fluxfeed-test-fanout-" + suffix,
		EmbeddingGroup: "fluxfeed-test-embedding-" + suffix,
	}
	kafka, err := NewKafka(cfg)
	if err != nil {
		t.Fatal(err)
	}
	bus := NewFeedEventBus(kafka, cfg)
	t.Cleanup(func() { _ = bus.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fanout := make(chan string, 1)
	embedding := make(chan string, 1)
	if err := bus.ConsumeVideoPublished(ctx, func(_ context.Context, event *applicationvideo.PublishedEvent) error {
		fanout <- event.EventID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.ConsumeVideoPublishedForEmbedding(ctx, func(_ context.Context, event *applicationvideo.PublishedEvent) error {
		embedding <- event.EventID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	event := &applicationvideo.PublishedEvent{EventID: "evt-" + suffix, VideoID: 1001, AuthorID: 42, OccurredAt: time.Now().UTC()}
	if err := bus.PublishVideoPublished(ctx, event); err != nil {
		t.Fatal(err)
	}
	for name, received := range map[string]<-chan string{"fanout": fanout, "embedding": embedding} {
		select {
		case eventID := <-received:
			if eventID != event.EventID {
				t.Fatalf("%s received %q, want %q", name, eventID, event.EventID)
			}
		case <-ctx.Done():
			t.Fatalf("%s consumer timed out: %v", name, ctx.Err())
		}
	}
}

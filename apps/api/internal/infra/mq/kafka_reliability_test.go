package inframq

import (
	applicationeventbus "FluxFeed/internal/application/eventbus"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type memoryDeduplicator struct {
	processed bool
	marked    int
}

func TestKafkaHeaderCarrierUpdatesAndReadsHeaders(t *testing.T) {
	record := &kgo.Record{Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("old")}}}
	carrier := kafkaHeaderCarrier{record: record}
	carrier.Set("TraceParent", "new")
	carrier.Set("tracestate", "state")

	if got := carrier.Get("traceparent"); got != "new" {
		t.Fatalf("traceparent = %q, want new", got)
	}
	if len(carrier.Keys()) != 2 {
		t.Fatalf("header count = %d, want 2", len(carrier.Keys()))
	}
}

func (d *memoryDeduplicator) IsProcessed(context.Context, string, string) (bool, error) {
	return d.processed, nil
}

func (d *memoryDeduplicator) MarkProcessed(context.Context, string, string) error {
	d.processed = true
	d.marked++
	return nil
}

func TestKafkaHandleEventDeduplicatesByConsumerGroup(t *testing.T) {
	dedup := &memoryDeduplicator{}
	kafka := &Kafka{dedup: dedup}
	event := &applicationeventbus.Event{ID: "evt-1", Type: applicationeventbus.TypeVideoPublished}
	handled := 0
	handler := func(context.Context, *applicationeventbus.Event) error {
		handled++
		return nil
	}
	if err := kafka.handleEvent(context.Background(), "fanout", event, handler); err != nil {
		t.Fatal(err)
	}
	if err := kafka.handleEvent(context.Background(), "fanout", event, handler); err != nil {
		t.Fatal(err)
	}
	if handled != 1 || dedup.marked != 1 {
		t.Fatalf("handled=%d marked=%d", handled, dedup.marked)
	}
}

func TestRunWithRetryUsesFiniteAttempts(t *testing.T) {
	wantErr := errors.New("failed")
	attempts, retries := 0, 0
	err := runWithRetry(context.Background(), 3, time.Nanosecond, func() error {
		attempts++
		return wantErr
	}, func() { retries++ })
	if !errors.Is(err, wantErr) || attempts != 4 || retries != 3 {
		t.Fatalf("err=%v attempts=%d retries=%d", err, attempts, retries)
	}
}

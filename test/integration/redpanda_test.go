//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
)

// redpandaImage is pinned so the job doesn't change under us.
const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v26.2.4"

// containerLabel marks every container these tests start, so `task
// test:integration` can check that none is left behind.
var containerLabel = map[string]string{"rpcn-debugger.test": "integration"}

// startTimeout bounds the image pull and broker startup. Hitting it returns an
// error, so t.Cleanup still runs; hitting go test's -timeout would skip it.
const startTimeout = 3 * time.Minute

// startRedpanda starts a single-node Redpanda broker that is removed when the
// test ends, and returns its seed broker address.
func startRedpanda(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), startTimeout)
	defer cancel()

	ctr, err := tcredpanda.Run(ctx, redpandaImage,
		tcredpanda.WithAutoCreateTopics(),
		testcontainers.WithLabels(containerLabel),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}

	broker, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	return broker
}

func TestRedpandaSmoke(t *testing.T) {
	broker := startRedpanda(t)

	const topic = "smoke"
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	producer, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	want := &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("hello")}
	if err := producer.ProduceSync(ctx, want).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	for {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("no record consumed: %v", err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("fetch: %v", errs[0].Err)
		}
		if recs := fetches.Records(); len(recs) > 0 {
			got := recs[0]
			if string(got.Key) != "k" || string(got.Value) != "hello" {
				t.Fatalf("got key=%q value=%q, want key=%q value=%q", got.Key, got.Value, "k", "hello")
			}
			return
		}
	}
}

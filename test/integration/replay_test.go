//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ajurcenk/rpcn-debugger/test/dapdrive"
)

// TestReplayBrokerDown is the AD-11 replay against a real broker: a
// capture from a topic is saved with saveCapture, the broker is stopped,
// and a replay of the file still steps the batch and delivers what the
// saving launch delivered, without reading the input.
func TestReplayBrokerDown(t *testing.T) {
	ctr, broker := startRedpandaContainer(t)

	const topic = "replay"
	want := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	adm := kadm.NewClient(cl)
	// One partition, so the records arrive in order in one fetch.
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		cl.Close()
		t.Fatalf("create topic: %v", err)
	}
	recs := make([]*kgo.Record, len(want))
	for i, v := range want {
		recs[i] = &kgo.Record{Topic: topic, Key: fmt.Appendf(nil, "k%d", i), Value: []byte(v)}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		cl.Close()
		t.Fatalf("produce: %v", err)
	}
	cl.Close()

	pipeline := filepath.Join(t.TempDir(), "pipeline.yaml")
	conf := fmt.Sprintf(`input:
  redpanda:
    seed_brokers: [%q]
    topics: [%q]
    consumer_group: prod

pipeline:
  processors:
    - mapping: 'root = this.merge({"seen": true})'

output:
  drop: {}
`, broker, topic)
	if err := os.WriteFile(pipeline, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}

	sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
	saved, err := dapdrive.Run(sctx, dapdrive.Options{
		Pipeline: pipeline, Script: dapdrive.Always(dapdrive.Continue),
		Launch: map[string]any{"saveCapture": "topic.json"},
	})
	scancel()
	if err != nil {
		t.Fatalf("saving session: %v", err)
	}
	if len(saved.Delivered) != 1 {
		t.Fatalf("saving session delivered %d batches, want 1; output:\n%s", len(saved.Delivered), strings.Join(saved.Output, ""))
	}
	var got []string
	for _, m := range saved.Delivered[0] {
		got = append(got, m.Content)
	}
	if wantSeen := []string{`{"n":1,"seen":true}`, `{"n":2,"seen":true}`, `{"n":3,"seen":true}`}; !slices.Equal(got, wantSeen) {
		t.Fatalf("saving session delivered %q, want %q", got, wantSeen)
	}

	stopTimeout := 30 * time.Second
	if err := ctr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop redpanda: %v", err)
	}

	sctx, scancel = context.WithTimeout(ctx, 30*time.Second)
	res, err := dapdrive.Run(sctx, dapdrive.Options{
		Pipeline: pipeline, Script: dapdrive.Always(dapdrive.Continue),
		Launch: map[string]any{"replay": ".rpcn-debug/topic.json"},
	})
	scancel()
	if err != nil {
		t.Fatalf("replay session: %v", err)
	}
	if !reflect.DeepEqual(res.Delivered, saved.Delivered) {
		t.Errorf("replay delivered %+v, want the saving session's %+v", res.Delivered, saved.Delivered)
	}
	if len(res.Stops) == 0 || !reflect.DeepEqual(res.Stops[0].Batches, saved.Stops[0].Batches) {
		t.Errorf("replay's first stop differs from the saving session's")
	}
	if notes := filterOutput(res.Output, "Replaying "); len(notes) != 1 {
		t.Errorf("replay notes = %q, want one; output:\n%s", notes, strings.Join(res.Output, ""))
	}
}

// filterOutput returns the lines of output that start with prefix.
func filterOutput(output []string, prefix string) []string {
	var out []string
	for _, o := range output {
		if strings.HasPrefix(o, prefix) {
			out = append(out, o)
		}
	}
	return out
}

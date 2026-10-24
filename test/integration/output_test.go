//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ajurcenk/rpcn-debugger/test/dapdrive"
)

// TestOutputModes is E3 entry 7 against a real broker (CAP-11, Done when
// 3): under output: real the topic holds exactly the batch the Debug
// Console showed, once, with output.processors applied once; a stdout
// beside it in a broker is replaced by drop; under none the topic gets
// nothing.
func TestOutputModes(t *testing.T) {
	broker := startRedpanda(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	want := []string{`{"n":1,"seen":1}`, `{"n":2,"seen":1}`}
	cases := []struct {
		name, mode, topic, output string
		// warnings must each be in one launch warning.
		warnings []string
		written  bool
	}{
		{
			name: "real", mode: "real", topic: "out-real", written: true,
			output:   "  redpanda:\n    seed_brokers: [%q]\n    topic: %s\n",
			warnings: []string{"output: real writes each delivered batch to the redpanda output at output (line 3)"},
		},
		{
			name: "stdout under real", mode: "real", topic: "out-broker", written: true,
			output: "  broker:\n    outputs:\n      - redpanda:\n          seed_brokers: [%q]\n          topic: %s\n      - stdout: {}\n",
			warnings: []string{
				"output: real writes each delivered batch to the broker output",
				"the stdout output is replaced by drop under output: real",
			},
		},
		{
			name: "none", mode: "none", topic: "out-none",
			output: "  redpanda:\n    seed_brokers: [%q]\n    topic: %s\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := adm.CreateTopic(ctx, 1, 1, nil, tc.topic); err != nil {
				t.Fatalf("create topic: %v", err)
			}
			pipeline := filepath.Join(t.TempDir(), "pipeline.yaml")
			conf := `input:
  generate: {count: 2, batch_size: 2, interval: "", mapping: 'root.n = counter()'}
output:
  processors:
    - mapping: 'root = this.merge({"seen": this.seen.or(0) + 1})'
` + fmt.Sprintf(tc.output, broker, tc.topic)
			if err := os.WriteFile(pipeline, []byte(conf), 0o600); err != nil {
				t.Fatal(err)
			}
			sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
			res, err := dapdrive.Run(sctx, dapdrive.Options{
				Pipeline: pipeline, Launch: map[string]any{"output": tc.mode}, Script: dapdrive.Always(dapdrive.Continue),
			})
			scancel()
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			if len(res.Delivered) != 1 {
				t.Fatalf("delivered %d batches, want 1; output:\n%s", len(res.Delivered), strings.Join(res.Output, ""))
			}
			var shown []string
			for _, m := range res.Delivered[0] {
				shown = append(shown, m.Content)
			}
			if !slices.Equal(shown, want) {
				t.Errorf("shown %q, want %q", shown, want)
			}
			out := strings.Join(res.Output, "")
			for _, w := range tc.warnings {
				if !strings.Contains(out, w) {
					t.Errorf("output lacks the warning %q:\n%s", w, out)
				}
			}
			if n, want := strings.Count(out, "output: real writes each delivered batch"), map[bool]int{true: 1}[tc.mode == "real"]; n != want {
				t.Errorf("%d real warnings, want %d:\n%s", n, want, out)
			}
			if !tc.written && strings.Contains(out, "output: real") {
				t.Errorf("output: %s warns about real:\n%s", tc.mode, out)
			}

			got := topicRecords(ctx, t, broker, adm, tc.topic)
			if !tc.written {
				if len(got) != 0 {
					t.Errorf("output: %s wrote %q to %s", tc.mode, got, tc.topic)
				}
				return
			}
			if !slices.Equal(got, shown) {
				t.Errorf("%s holds %q, want exactly the shown batch %q", tc.topic, got, shown)
			}
		})
	}
}

// topicRecords returns the values of every record in topic, read up to its
// end offsets.
func topicRecords(ctx context.Context, t *testing.T, broker string, adm *kadm.Client, topic string) []string {
	t.Helper()
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("end offsets of %s: %v", topic, err)
	}
	total := int64(0)
	ends.Each(func(o kadm.ListedOffset) { total += o.Offset })
	if total == 0 {
		return nil
	}
	cons, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer cons.Close()
	var got []string
	for int64(len(got)) < total {
		fetches := cons.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("read %s: %v (have %q of %d)", topic, err, got, total)
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	return got
}

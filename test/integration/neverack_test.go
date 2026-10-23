//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ajurcenk/rpcn-debugger/test/dapdrive"
)

// prodGroup is the pipeline's own consumer group, which a debug session
// must leave alone.
const prodGroup = "prod"

// debugGroupWarning is the launch warning naming the replaced group and the
// debug group the capture uses instead (AD-5).
var debugGroupWarning = regexp.MustCompile(`consumer group "` + prodGroup + `" is replaced by the debug group "([^"]+)"`)

// TestNeverAck is the never-ack test (AD-8, Done when 4 of E3): two debug
// sessions in a row against a real topic each deliver the same records,
// neither the production group nor the debug group has a committed offset
// after either session, and a live production member sees no rebalance, so
// the capture never joined the production group.
func TestNeverAck(t *testing.T) {
	broker := startRedpanda(t)

	const topic = "neverack"
	want := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	// One partition, so the records arrive in order in one fetch.
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	recs := make([]*kgo.Record, len(want))
	for i, v := range want {
		recs[i] = &kgo.Record{Topic: topic, Value: []byte(v)}
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	member, memberID, generation := startProdMember(ctx, t, broker, adm, topic)

	pipeline := filepath.Join(t.TempDir(), "pipeline.yaml")
	conf := fmt.Sprintf(`input:
  redpanda:
    seed_brokers: [%q]
    topics: [%q]
    consumer_group: %s

pipeline:
  processors:
    - mapping: root = this

output:
  drop: {}
`, broker, topic, prodGroup)
	if err := os.WriteFile(pipeline, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}

	var debugGroup string
	for session := 1; session <= 2; session++ {
		// A capture that joined prod gets no partition (the production
		// member holds the only one) and waits for ever, so each session
		// has its own deadline.
		sctx, scancel := context.WithTimeout(ctx, 30*time.Second)
		res, err := dapdrive.Run(sctx, dapdrive.Options{Pipeline: pipeline, Script: dapdrive.Always(dapdrive.Continue)})
		scancel()
		if err != nil {
			t.Fatalf("session %d: %v (a capture that waits for ever may have joined %q)", session, err, prodGroup)
		}
		if len(res.Delivered) != 1 {
			t.Fatalf("session %d delivered %d batches, want 1; output:\n%s", session, len(res.Delivered), strings.Join(res.Output, ""))
		}
		var got []string
		for _, m := range res.Delivered[0] {
			got = append(got, m.Content)
		}
		if !slices.Equal(got, want) {
			t.Errorf("session %d delivered %q, want %q", session, got, want)
		}
		group := ""
		for _, o := range res.Output {
			if m := debugGroupWarning.FindStringSubmatch(o); m != nil {
				group = m[1]
			}
		}
		if group == "" || !strings.HasPrefix(group, "rpcn-debug-") || !strings.HasSuffix(group, "-0") {
			t.Fatalf("session %d: no warning naming the debug group; output:\n%s", session, strings.Join(res.Output, ""))
		}
		if debugGroup != "" && group != debugGroup {
			t.Errorf("session %d used debug group %q, session 1 used %q", session, group, debugGroup)
		}
		debugGroup = group

		// Checked after each session, so a session that commits fails here
		// rather than as a timeout of the next one.
		for _, g := range []string{prodGroup, debugGroup} {
			checkNoCommits(ctx, t, adm, g)
		}
		if t.Failed() {
			t.FailNow()
		}
	}

	// A capture that joined prod would have rebalanced it, bumping the
	// member's generation.
	if _, gen := member.GroupMetadata(); gen != generation {
		t.Errorf("the production member's generation went from %d to %d: the capture joined %q", generation, gen, prodGroup)
	}
	groups, err := adm.DescribeGroups(ctx, prodGroup)
	if err != nil {
		t.Fatalf("describe %s: %v", prodGroup, err)
	}
	if g := groups[prodGroup]; len(g.Members) != 1 || g.Members[0].MemberID != memberID {
		t.Errorf("group %s has members %+v, want only the production member %s", prodGroup, g.Members, memberID)
	}
}

// startProdMember starts a live member of the production group that
// consumes topic without committing, until the test ends. It waits until the
// group is Stable with that one member, and returns the member's client, its
// member ID and its generation.
func startProdMember(ctx context.Context, t *testing.T, broker string, adm *kadm.Client, topic string) (*kgo.Client, string, int32) {
	t.Helper()
	member, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(prodGroup),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("production member: %v", err)
	}
	pollCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for pollCtx.Err() == nil {
			member.PollFetches(pollCtx)
		}
	}()
	t.Cleanup(func() {
		stop()
		<-done
		member.Close()
	})

	for {
		groups, err := adm.DescribeGroups(ctx, prodGroup)
		if err == nil {
			g := groups[prodGroup]
			id, gen := member.GroupMetadata()
			if g.State == "Stable" && len(g.Members) == 1 && g.Members[0].MemberID == id && gen > 0 {
				return member, id, gen
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("group %s never became Stable with one member: %v", prodGroup, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// checkNoCommits fails the test when group has a committed offset.
func checkNoCommits(ctx context.Context, t *testing.T, adm *kadm.Client, group string) {
	t.Helper()
	offsets, err := adm.FetchOffsets(ctx, group)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return
	}
	if err != nil {
		t.Fatalf("fetch offsets of %s: %v", group, err)
	}
	offsets.Each(func(o kadm.OffsetResponse) {
		if o.Err != nil {
			t.Errorf("group %s, %s/%d: %v", group, o.Topic, o.Partition, o.Err)
		} else if o.At >= 0 {
			t.Errorf("group %s has a committed offset %d on %s/%d", group, o.At, o.Topic, o.Partition)
		}
	})
}

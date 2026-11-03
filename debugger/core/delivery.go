package core

import (
	"context"
	"fmt"
	"time"
)

// nothingDelivered is the end-of-run note when the sink received no batch,
// in the words of output mode m (AD-21).
func nothingDelivered(m OutputMode) string {
	switch m.Effective() {
	case OutputReal:
		return "Nothing was delivered, so nothing was written to the output"
	case OutputNone:
		return "Nothing was delivered (output: none)"
	default:
		return "Nothing was delivered to dry-run output"
	}
}

// capture runs the engine's Capture and keeps the batch it returns, for
// Restart (AD-20). A quiet input would leave the user waiting without a
// word, so after captureWait with nothing captured it sends one waiting note
// (AD-4).
func (s *Session) capture(ctx context.Context, opts StartOptions) (CapturedBatch, error) {
	noted := make(chan struct{})
	s.mu.Lock()
	delay := s.captureWait
	s.mu.Unlock()
	wait := time.AfterFunc(delay, func() {
		defer close(noted)
		s.waitingNote()
	})
	b, err := s.engine.Capture(ctx, opts)
	if err == nil {
		s.keepCaptured(b)
	}
	if !wait.Stop() {
		<-noted // the note goes out before the run's last events
	}
	return b, err
}

// keepCaptured keeps b as the session's captured batch. When the capture
// dropped messages for maxMessages, it tells the user how many it kept.
func (s *Session) keepCaptured(b CapturedBatch) {
	s.mu.Lock()
	s.captured, s.kept = true, &b
	ended := s.ended
	s.mu.Unlock()
	if ended || b.Kept >= b.Total {
		return
	}
	s.fe.Emit(Output{Category: "console", Text: fmt.Sprintf("captured %d of %d messages (maxMessages)\n", b.Kept, b.Total)})
}

// SetCaptureWait sets how long the run may capture nothing before the
// Session's waiting note (AD-4: 5 s). It is for tests, and must be called
// before ConfigurationDone.
func (s *Session) SetCaptureWait(d time.Duration) {
	s.mu.Lock()
	s.captureWait = d
	s.mu.Unlock()
}

// waitingNote tells the user, once, that the run has captured nothing yet.
// It sends nothing once Captured was called or the session or run ended.
func (s *Session) waitingNote() {
	s.mu.Lock()
	quiet := !s.captured && !s.ended && !s.runDone
	delay := s.captureWait
	s.mu.Unlock()
	if quiet {
		s.fe.Emit(Output{Category: "console", Text: fmt.Sprintf("Waiting for the input: nothing captured yet after %g s.\n", delay.Seconds())})
	}
}

// Deliver implements Arriver: it numbers batch, from 1 in the run (AD-3;
// Restart starts the count again), and emits it as one Delivered event with its outcome and write
// error, in the order the sink received it (AD-21). Its Snapshot is
// positioned at the user path output, on the line of the top-level output
// key, on the root thread; it has no frame or stepper (FrameID 0,
// StepperID -1).
func (s *Session) Deliver(_ context.Context, batch Batch, outcome Outcome, err error) {
	if batch.Messages == nil {
		batch.Messages = []Message{}
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.delivered++
	n, tr := s.delivered, s.translate
	pos := Position{ThreadID: ThreadID, StepperID: -1, UserPath: s.delivery.UserPath, File: s.pipeline, Line: s.delivery.Line}
	s.mu.Unlock()
	if pos.UserPath == "" {
		pos.UserPath = "output"
	}
	ev := Delivered{Batch: n, Outcome: outcome, Snapshot: Snapshot{SchemaVersion: SchemaVersion, Position: pos, Batches: []Batch{batch}}}
	if err != nil {
		ev.Error = TranslateText(tr, err.Error())
	}
	s.fe.Emit(ev)
}

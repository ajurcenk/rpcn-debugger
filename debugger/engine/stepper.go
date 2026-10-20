package engine

import (
	"context"
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// StepperName is the stepper plugin the rewrite wraps child lists in.
const StepperName = "rpcn_debug_stepper"

func stepperSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Runs its child processors one at a time and stops before each one (rpcn-debugger).").
		Field(service.NewIntField("id")).
		// parent, mode, strict and concurrent come from the rewrite (AD-7,
		// AD-16). They are accepted and stored, and acted on with frames.
		Field(service.NewIntField("parent").Default(-1)).
		Field(service.NewStringEnumField("mode", "all", "try").Default("all")).
		Field(service.NewBoolField("strict").Default(false)).
		Field(service.NewBoolField("concurrent").Default(false)).
		Field(service.NewIntListField("lines").Default([]any{})).
		Field(service.NewProcessorListField("processors"))
}

// registerStepper registers rpcn_debug_stepper on env. Every stepper built
// from env reports its stop positions to a.
func registerStepper(env *service.Environment, a core.Arriver) error {
	return env.RegisterBatchProcessor(StepperName, stepperSpec(),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
			s := &stepper{arriver: a}
			var err error
			if s.id, err = conf.FieldInt("id"); err != nil {
				return nil, err
			}
			if s.parent, err = conf.FieldInt("parent"); err != nil {
				return nil, err
			}
			if s.mode, err = conf.FieldString("mode"); err != nil {
				return nil, err
			}
			if s.strict, err = conf.FieldBool("strict"); err != nil {
				return nil, err
			}
			if s.concurrent, err = conf.FieldBool("concurrent"); err != nil {
				return nil, err
			}
			// Children are built only through the stepper's own field (AD-4).
			if s.procs, err = conf.FieldProcessorList("processors"); err != nil {
				return nil, err
			}
			return s, nil
		})
}

// stepper mirrors ExecuteAll in mode "all": each child runs on every batch
// the previous one produced. Each ProcessBatch is one entry (AD-17): it
// enters its frame, which goes on ctx for every Arrive and every child, and
// exits it on return. It calls Arrive before each child and once after the
// last (childIndex = len(procs), AD-10), with Snapshot batches deep-copied
// from all of them (AD-19). The gate is never held while a child runs: it
// is held only inside Arrive (AD-9).
//
// parent, mode, strict and concurrent are stored but not acted on yet: a
// "try" stepper runs like "all" until entry 2.7.
type stepper struct {
	id         int
	parent     int
	mode       string
	strict     bool
	concurrent bool
	procs      []*service.OwnedProcessor
	arriver    core.Arriver
}

func (s *stepper) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	ctx, exit := s.arriver.Enter(ctx, s.id, len(s.procs))
	defer exit()

	batches := []service.MessageBatch{batch}
	for i, p := range s.procs {
		if err := s.arriver.Arrive(ctx, s.id, i, snapshotBatches(batches)); err != nil {
			// The session ended: deliver nothing.
			return nil, nil
		}
		var next []service.MessageBatch
		for _, b := range batches {
			out, err := p.ProcessBatch(ctx, b)
			if err != nil {
				return nil, err
			}
			next = append(next, out...)
		}
		batches = next
		if len(batches) == 0 {
			break
		}
	}
	// The end-of-entry stop only observes: it runs even when a child left
	// no batches, and changes nothing (AD-8).
	if err := s.arriver.Arrive(ctx, s.id, len(s.procs), snapshotBatches(batches)); err != nil {
		return nil, nil
	}
	return batches, nil
}

func (s *stepper) Close(ctx context.Context) error {
	var errs []error
	for _, p := range s.procs {
		errs = append(errs, p.Close(ctx))
	}
	return errors.Join(errs...)
}

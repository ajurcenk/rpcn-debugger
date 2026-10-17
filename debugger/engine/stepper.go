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
		Field(service.NewIntListField("lines").Default([]any{})).
		Field(service.NewProcessorListField("processors"))
}

// registerStepper registers rpcn_debug_stepper on env. Every stepper built
// from env reports its stop positions to a.
func registerStepper(env *service.Environment, a core.Arriver) error {
	return env.RegisterBatchProcessor(StepperName, stepperSpec(),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
			id, err := conf.FieldInt("id")
			if err != nil {
				return nil, err
			}
			// Children are built only through the stepper's own field (AD-4).
			procs, err := conf.FieldProcessorList("processors")
			if err != nil {
				return nil, err
			}
			return &stepper{id: id, procs: procs, arriver: a}, nil
		})
}

// stepper mirrors ExecuteAll in mode "all": each child runs on every batch
// the previous one produced. Before each child it calls Arrive once, with
// Snapshot batches deep-copied from all of them (AD-19). The gate is never held while a child runs: it
// is held only inside Arrive (AD-9).
type stepper struct {
	id      int
	procs   []*service.OwnedProcessor
	arriver core.Arriver
}

func (s *stepper) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
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
	return batches, nil
}

func (s *stepper) Close(ctx context.Context) error {
	var errs []error
	for _, p := range s.procs {
		errs = append(errs, p.Close(ctx))
	}
	return errors.Join(errs...)
}

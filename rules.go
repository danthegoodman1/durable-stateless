package durablestateless

import (
	"reflect"

	"github.com/qmuntal/stateless"
)

type Rules struct {
	sm *stateless.StateMachine
}

func newRules(sm *stateless.StateMachine) *Rules {
	return &Rules{sm: sm}
}

func (r *Rules) Configure(state stateless.State) *StateRules {
	return &StateRules{config: r.sm.Configure(state)}
}

func (r *Rules) SetTriggerParameters(trigger stateless.Trigger, argumentTypes ...reflect.Type) {
	r.sm.SetTriggerParameters(trigger, argumentTypes...)
}

func (r *Rules) OnUnhandledTrigger(fn stateless.UnhandledTriggerActionFunc) {
	r.sm.OnUnhandledTrigger(fn)
}

type StateRules struct {
	config *stateless.StateConfiguration
}

func (r *StateRules) State() stateless.State {
	return r.config.State()
}

func (r *StateRules) Permit(trigger stateless.Trigger, destinationState stateless.State, guards ...stateless.GuardFunc) *StateRules {
	r.config.Permit(trigger, destinationState, guards...)
	return r
}

func (r *StateRules) PermitDynamic(trigger stateless.Trigger, selector stateless.DestinationSelectorFunc, guards ...stateless.GuardFunc) *StateRules {
	r.config.PermitDynamic(trigger, selector, guards...)
	return r
}

func (r *StateRules) PermitReentry(trigger stateless.Trigger, guards ...stateless.GuardFunc) *StateRules {
	r.config.PermitReentry(trigger, guards...)
	return r
}

func (r *StateRules) Ignore(trigger stateless.Trigger, guards ...stateless.GuardFunc) *StateRules {
	r.config.Ignore(trigger, guards...)
	return r
}

func (r *StateRules) SubstateOf(superstate stateless.State) *StateRules {
	r.config.SubstateOf(superstate)
	return r
}

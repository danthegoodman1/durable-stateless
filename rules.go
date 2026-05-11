package durablestateless

import (
	"reflect"

	"github.com/qmuntal/stateless"
)

// Rules is the safe configuration surface for stateless transition legality.
// It intentionally does not expose OnEntry or OnExit hooks.
type Rules struct {
	sm *stateless.StateMachine
}

func newRules(sm *stateless.StateMachine) *Rules {
	return &Rules{sm: sm}
}

// Configure starts configuring transitions for state.
func (r *Rules) Configure(state stateless.State) *StateRules {
	return &StateRules{config: r.sm.Configure(state)}
}

// SetTriggerParameters declares the expected argument types for a trigger.
func (r *Rules) SetTriggerParameters(trigger stateless.Trigger, argumentTypes ...reflect.Type) {
	r.sm.SetTriggerParameters(trigger, argumentTypes...)
}

// OnUnhandledTrigger configures stateless behavior for unhandled triggers.
func (r *Rules) OnUnhandledTrigger(fn stateless.UnhandledTriggerActionFunc) {
	r.sm.OnUnhandledTrigger(fn)
}

// StateRules configures legal transitions from one state.
type StateRules struct {
	config *stateless.StateConfiguration
}

// State returns the state being configured.
func (r *StateRules) State() stateless.State {
	return r.config.State()
}

// Permit allows trigger to transition to destinationState.
func (r *StateRules) Permit(trigger stateless.Trigger, destinationState stateless.State, guards ...stateless.GuardFunc) *StateRules {
	r.config.Permit(trigger, destinationState, guards...)
	return r
}

// PermitDynamic allows trigger to choose a destination with selector.
func (r *StateRules) PermitDynamic(trigger stateless.Trigger, selector stateless.DestinationSelectorFunc, guards ...stateless.GuardFunc) *StateRules {
	r.config.PermitDynamic(trigger, selector, guards...)
	return r
}

// PermitReentry allows trigger to re-enter the current state.
func (r *StateRules) PermitReentry(trigger stateless.Trigger, guards ...stateless.GuardFunc) *StateRules {
	r.config.PermitReentry(trigger, guards...)
	return r
}

// Ignore treats trigger as a legal no-op.
func (r *StateRules) Ignore(trigger stateless.Trigger, guards ...stateless.GuardFunc) *StateRules {
	r.config.Ignore(trigger, guards...)
	return r
}

// SubstateOf marks this state as a substate of superstate.
func (r *StateRules) SubstateOf(superstate stateless.State) *StateRules {
	r.config.SubstateOf(superstate)
	return r
}

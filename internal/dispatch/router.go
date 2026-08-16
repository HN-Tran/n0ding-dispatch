package dispatch

import (
	"errors"
	"sort"
)

type AgentState struct {
	Active      int
	Quarantined bool
}
type Candidate struct {
	AgentID  string   `json:"agent_id"`
	Eligible bool     `json:"eligible"`
	Reasons  []string `json:"reasons"`
	Score    int      `json:"score"`
}
type RouteDecision struct {
	Selected      string      `json:"selected"`
	Candidates    []Candidate `json:"candidates"`
	PolicyVersion string      `json:"policy_version"`
}

func Route(task Task, agents []Agent, states map[string]AgentState, policyVersion string) (RouteDecision, error) {
	if policyVersion == "" {
		return RouteDecision{}, errors.New("policy version required")
	}
	req := map[string]bool{}
	for _, c := range task.Requires {
		req[c] = true
	}
	decision := RouteDecision{PolicyVersion: policyVersion}
	for _, a := range agents {
		c := Candidate{AgentID: a.ID, Eligible: true, Score: a.Priority}
		if a.ID == "" || a.Version == "" {
			c.Eligible = false
			c.Reasons = append(c.Reasons, "invalid_agent")
		}
		if !a.Enabled {
			c.Eligible = false
			c.Reasons = append(c.Reasons, "disabled")
		}
		st := states[a.ID]
		if st.Quarantined {
			c.Eligible = false
			c.Reasons = append(c.Reasons, "quarantined")
		}
		if a.MaxConcurrent <= 0 || st.Active >= a.MaxConcurrent {
			c.Eligible = false
			c.Reasons = append(c.Reasons, "capacity_exhausted")
		}
		has := map[string]bool{}
		for _, x := range a.Capabilities {
			has[x] = true
		}
		for need := range req {
			if !has[need] {
				c.Eligible = false
				c.Reasons = append(c.Reasons, "missing_capability:"+need)
			}
		}
		sort.Strings(c.Reasons)
		decision.Candidates = append(decision.Candidates, c)
	}
	sort.Slice(decision.Candidates, func(i, j int) bool {
		a, b := decision.Candidates[i], decision.Candidates[j]
		if a.Eligible != b.Eligible {
			return a.Eligible
		}
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.AgentID < b.AgentID
	})
	for _, c := range decision.Candidates {
		if c.Eligible {
			decision.Selected = c.AgentID
			return decision, nil
		}
	}
	return decision, errors.New("no eligible agent")
}

package dispatch

import (
	"errors"
	"fmt"
	"sort"
)

const SchemaVersion = "dispatch.n0ding.dev/v1"

type Capability struct {
	Name, Version string
	SideEffecting bool
}

type Agent struct {
	ID, Version   string
	Capabilities  []string
	Priority      int
	Enabled       bool
	MaxConcurrent int
}

type Task struct {
	ID, Version string
	Requires    []string
	DependsOn   []string
	Cost        uint64
}

type TaskDAG struct {
	Version   string
	Tasks     []Task
	MaxFanout int
	Budget    uint64
}

type Catalog struct {
	Version      string
	Capabilities []Capability
	Agents       []Agent
}

func (c Catalog) Validate() error {
	if c.Version == "" {
		return errors.New("catalog version required")
	}
	caps := map[string]bool{}
	for _, capability := range c.Capabilities {
		if capability.Name == "" || capability.Version == "" {
			return errors.New("capability name and version required")
		}
		key := capability.Name + "@" + capability.Version
		if caps[key] {
			return fmt.Errorf("duplicate capability %q", key)
		}
		caps[key] = true
	}
	agents := map[string]bool{}
	for _, agent := range c.Agents {
		if agent.ID == "" || agent.Version == "" {
			return errors.New("agent id and version required")
		}
		key := agent.ID + "@" + agent.Version
		if agents[key] {
			return fmt.Errorf("duplicate agent %q", key)
		}
		agents[key] = true
		for _, capability := range agent.Capabilities {
			if !caps[capability] {
				return fmt.Errorf("agent %q references unknown capability %q", agent.ID, capability)
			}
		}
	}
	return nil
}

func (d TaskDAG) Validate() error {
	if d.Version == "" {
		return errors.New("DAG version required")
	}
	if d.MaxFanout <= 0 {
		return errors.New("positive fanout bound required")
	}
	ids := map[string]Task{}
	var cost uint64
	for _, t := range d.Tasks {
		if t.ID == "" || t.Version == "" {
			return errors.New("task id and version required")
		}
		if _, exists := ids[t.ID]; exists {
			return fmt.Errorf("duplicate task %q", t.ID)
		}
		ids[t.ID] = t
		if ^uint64(0)-cost < t.Cost {
			return errors.New("cost overflow")
		}
		cost += t.Cost
	}
	if cost > d.Budget {
		return fmt.Errorf("budget exceeded: %d > %d", cost, d.Budget)
	}
	children := map[string]int{}
	for _, t := range d.Tasks {
		seen := map[string]bool{}
		for _, dep := range t.DependsOn {
			if _, ok := ids[dep]; !ok {
				return fmt.Errorf("task %q depends on unknown task %q", t.ID, dep)
			}
			if seen[dep] {
				return fmt.Errorf("duplicate dependency %q -> %q", dep, t.ID)
			}
			seen[dep] = true
			children[dep]++
			if children[dep] > d.MaxFanout {
				return fmt.Errorf("fanout exceeded for %q", dep)
			}
		}
	}
	state := map[string]uint8{}
	var visit func(string) error
	visit = func(id string) error {
		if state[id] == 1 {
			return fmt.Errorf("cycle involving %q", id)
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		for _, dep := range ids[id].DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	keys := make([]string, 0, len(ids))
	for id := range ids {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

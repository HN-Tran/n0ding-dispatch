package dispatch

import "testing"

func TestCatalogVersionedReferences(t *testing.T) {
	c := Catalog{Version: "v1", Capabilities: []Capability{{Name: "read", Version: "v2"}}, Agents: []Agent{{ID: "a", Version: "v3", Capabilities: []string{"read@v2"}}}}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Agents[0].Capabilities = []string{"write@v1"}
	if c.Validate() == nil {
		t.Fatal("unknown capability accepted")
	}
}

func validDAG() TaskDAG {
	return TaskDAG{Version: "v1", MaxFanout: 2, Budget: 10, Tasks: []Task{{ID: "a", Version: "v1", Cost: 2}, {ID: "b", Version: "v1", DependsOn: []string{"a"}, Cost: 3}}}
}
func TestDAGValidation(t *testing.T) {
	if err := validDAG().Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		mut  func(*TaskDAG)
	}{
		{"cycle", func(d *TaskDAG) { d.Tasks[0].DependsOn = []string{"b"} }},
		{"budget", func(d *TaskDAG) { d.Budget = 1 }},
		{"unknown", func(d *TaskDAG) { d.Tasks[1].DependsOn = []string{"x"} }},
		{"duplicate", func(d *TaskDAG) { d.Tasks = append(d.Tasks, d.Tasks[0]) }},
		{"fanout", func(d *TaskDAG) {
			d.MaxFanout = 1
			d.Tasks = append(d.Tasks, Task{ID: "c", Version: "v1", DependsOn: []string{"a"}})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := validDAG()
			tt.mut(&d)
			if d.Validate() == nil {
				t.Fatal("invalid DAG accepted")
			}
		})
	}
}
func TestRouterAuditAndStableTieBreak(t *testing.T) {
	task := Task{ID: "t", Version: "v1", Requires: []string{"read"}}
	agents := []Agent{{ID: "z", Version: "v1", Capabilities: []string{"read"}, Priority: 1, Enabled: true, MaxConcurrent: 1}, {ID: "a", Version: "v1", Capabilities: []string{"read"}, Priority: 1, Enabled: true, MaxConcurrent: 1}, {ID: "bad", Version: "v1", Priority: 99, Enabled: true, MaxConcurrent: 1}}
	d, err := Route(task, agents, map[string]AgentState{}, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if d.Selected != "a" {
		t.Fatalf("selected %s", d.Selected)
	}
	if len(d.Candidates) != 3 {
		t.Fatal("missing audit candidates")
	}
	found := false
	for _, c := range d.Candidates {
		if c.AgentID == "bad" {
			found = true
			if c.Eligible || len(c.Reasons) == 0 {
				t.Fatal("missing exclusion")
			}
		}
	}
	if !found {
		t.Fatal("candidate omitted")
	}
}
func TestRouterNoEligibleStillReturnsEvidence(t *testing.T) {
	d, err := Route(Task{ID: "t", Version: "v1", Requires: []string{"x"}}, []Agent{{ID: "a", Version: "v1", Enabled: false, MaxConcurrent: 1}}, nil, "p")
	if err == nil || len(d.Candidates) != 1 {
		t.Fatal(d, err)
	}
}

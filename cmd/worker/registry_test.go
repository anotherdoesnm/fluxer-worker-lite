package main

// Guards against the quietest failure mode this worker has: a task is listed as
// handled in OMISSIONS.md, somebody forgets the registry line, and the task is
// then parked at runtime while every document says it runs. Nothing crashes — the
// work simply never happens.
//
// The test reads the "Handled today" table from OMISSIONS.md and requires a
// handler for every task name in its first column.

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestEveryHandledTaskInOmissionsHasAHandler(t *testing.T) {
	raw, err := os.ReadFile("../../OMISSIONS.md")
	if err != nil {
		t.Fatalf("read OMISSIONS.md: %v", err)
	}
	text := string(raw)
	start := strings.Index(text, "## 1. Handled today")
	if start < 0 {
		t.Fatal("OMISSIONS.md has no \"Handled today\" section")
	}
	section := text[start:]
	if end := strings.Index(section, "\n## 2."); end >= 0 {
		section = section[:end]
	}

	known := make(map[string]struct{})
	for _, lane := range lanes {
		for _, task := range lane.Tasks {
			known[task] = struct{}{}
		}
		for _, task := range lane.Retired {
			known[task] = struct{}{}
		}
	}

	nameInRow := regexp.MustCompile("`([A-Za-z0-9]+)`")
	checked := 0
	for _, line := range strings.Split(section, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "|") {
			continue
		}
		// First column only: the notes column mentions other tasks on purpose.
		cols := strings.Split(line, "|")
		if len(cols) < 3 {
			continue
		}
		for _, m := range nameInRow.FindAllStringSubmatch(cols[1], -1) {
			task := m[1]
			if _, ok := known[task]; !ok {
				continue
			}
			if _, ok := handlers[task]; !ok {
				t.Errorf("OMISSIONS.md lists %s as handled, but handlers[] has no entry for it", task)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("found no task names in the handled section; the parser or the document changed shape")
	}
}

func TestHandlersAreKnownTasks(t *testing.T) {
	known := make(map[string]struct{})
	for _, lane := range lanes {
		for _, task := range lane.Tasks {
			known[task] = struct{}{}
		}
	}
	for task := range handlers {
		if _, ok := known[task]; !ok {
			t.Errorf("handler %q is not a task in any lane: a typo here parks the real task", task)
		}
	}
}

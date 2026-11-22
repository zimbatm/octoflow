package main

import "testing"

func TestCompactRefusesBadDays(t *testing.T) {
	for _, args := range [][]string{
		{"--day", "yesterday"},
		{"--day", "2026-11-21", "--from", "2026-11-20"},
		{"--to", "2026-11-21"},
		{"--from", "2026-11-21", "--to", "2026-11-20"},
		{"--from", "2999-01-01"},
		{"--delete-batches"},
	} {
		if code := runCompact(args); code != 2 {
			t.Errorf("octoflow compact %v: exit %d, want 2", args, code)
		}
	}
}

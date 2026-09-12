package controller

import (
	"strings"
	"testing"
	"time"
)

func at(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func nextOf(t *testing.T, expr, after string) string {
	t.Helper()
	sched, err := ParseCron(expr)
	if err != nil {
		t.Fatal(err)
	}
	next, ok := sched.Next(at(t, after))
	if !ok {
		t.Fatalf("expression %q never matches after %s", expr, after)
	}
	return next.UTC().Format("2006-01-02 15:04")
}

func TestCronNextEveryMinute(t *testing.T) {
	if got := nextOf(t, "* * * * *", "2026-09-12T10:04:30Z"); got != "2026-09-12 10:05" {
		t.Fatalf("next = %s", got)
	}
}

func TestCronNextDaily(t *testing.T) {
	cases := []struct{ after, want string }{
		{"2026-09-12T06:59:00Z", "2026-09-12 07:00"},
		{"2026-09-12T07:00:00Z", "2026-09-13 07:00"},
		{"2026-09-12T08:30:00Z", "2026-09-13 07:00"},
	}
	for _, tc := range cases {
		if got := nextOf(t, "0 7 * * *", tc.after); got != tc.want {
			t.Fatalf("after %s: next = %s, want %s", tc.after, got, tc.want)
		}
	}
}

func TestCronStepsAndRanges(t *testing.T) {
	if got := nextOf(t, "*/5 * * * *", "2026-09-12T10:03:00Z"); got != "2026-09-12 10:05" {
		t.Fatalf("step next = %s", got)
	}
	if got := nextOf(t, "0 9-17 * * *", "2026-09-12T18:00:00Z"); got != "2026-09-13 09:00" {
		t.Fatalf("range next = %s", got)
	}
	if got := nextOf(t, "0 8,12,18 * * *", "2026-09-12T12:30:00Z"); got != "2026-09-12 18:00" {
		t.Fatalf("list next = %s", got)
	}
	if got := nextOf(t, "30 4 1 * *", "2026-09-12T00:00:00Z"); got != "2026-10-01 04:30" {
		t.Fatalf("dom next = %s", got)
	}
}

func TestCronDomDowOrRule(t *testing.T) {
	if got := nextOf(t, "0 0 13 * 5", "2026-09-12T00:00:00Z"); got != "2026-09-13 00:00" {
		t.Fatalf("dom-or-dow: 13th is Sunday, want the 13th: %s", got)
	}
	if got := nextOf(t, "0 0 13 * 5", "2026-09-14T00:00:00Z"); got != "2026-09-18 00:00" {
		t.Fatalf("dom-or-dow: next Friday: %s", got)
	}
}

func TestCronMonthWrapAndLeapDay(t *testing.T) {
	if got := nextOf(t, "0 0 1 12 *", "2026-12-02T00:00:00Z"); got != "2027-12-01 00:00" {
		t.Fatalf("month wrap = %s", got)
	}
	if got := nextOf(t, "0 0 29 2 *", "2026-03-01T00:00:00Z"); got != "2028-02-29 00:00" {
		t.Fatalf("leap day = %s", got)
	}
}

func TestCronSevenSunday(t *testing.T) {
	if got := nextOf(t, "0 0 * * 7", "2026-09-12T00:00:00Z"); got != "2026-09-13 00:00" {
		t.Fatalf("7 as sunday = %s", got)
	}
}

func TestCronInvalidExpressions(t *testing.T) {
	for _, expr := range []string{
		"99 * * * *",
		"not-a-cron",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 32 * *",
		"* * * 13 *",
		"* * * * 8",
		"*/0 * * * *",
		"a * * * *",
		"1-99 * * * *",
		"99-1 * * * *",
		"* * * * * extra",
		"",
		"*/ * * * *",
	} {
		if _, err := ParseCron(expr); err == nil {
			t.Fatalf("expected rejection for %q", expr)
		} else if !strings.Contains(err.Error(), "GRAPH_INVALID") {
			t.Fatalf("error for %q = %v", expr, err)
		}
	}
}

func TestCronNeverMatches(t *testing.T) {
	sched, err := ParseCron("0 0 31 2 *")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sched.Next(at(t, "2026-01-01T00:00:00Z")); ok {
		t.Fatal("Feb 31 must never match")
	}
}

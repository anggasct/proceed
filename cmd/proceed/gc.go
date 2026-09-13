package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"proceed/internal/store"
)

const gcUsage = `usage:
  proceed gc --keep-runs <n> --older-than <duration> [--dry-run] [--yes] [--data-dir <dir>] [--config <file>]
`

type gcFlags struct {
	keepRuns   string
	olderThan  string
	dryRun     bool
	yes        bool
	configPath string
	dataDir    string
}

func parseGCFlags(args []string) (gcFlags, error) {
	var f gcFlags
	for i := 0; i < len(args); i++ {
		arg := args[i]
		value := ""
		hasValue := false
		if eq := strings.IndexByte(arg, '='); eq > 0 && strings.HasPrefix(arg, "--") {
			value = arg[eq+1:]
			arg = arg[:eq]
			hasValue = true
		}
		switch arg {
		case "--keep-runs", "--older-than", "--data-dir", "--config":
			if !hasValue {
				if i+1 >= len(args) {
					return f, fmt.Errorf("%s requires a value", arg)
				}
				i++
				value = args[i]
			}
			switch arg {
			case "--keep-runs":
				f.keepRuns = value
			case "--older-than":
				f.olderThan = value
			case "--data-dir":
				f.dataDir = value
			case "--config":
				f.configPath = value
			}
		case "--dry-run":
			if hasValue {
				return f, fmt.Errorf("--dry-run takes no value")
			}
			f.dryRun = true
		case "--yes":
			if hasValue {
				return f, fmt.Errorf("--yes takes no value")
			}
			f.yes = true
		case "-h", "--help":
			return f, fmt.Errorf("help")
		default:
			return f, fmt.Errorf("unexpected argument %q", arg)
		}
	}
	return f, nil
}

func parseGCDuration(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, fmt.Errorf("invalid --older-than %q: duration is required (e.g. 90d, 12h, 30m)", raw)
	}
	unit := trimmed[len(trimmed)-1]
	number := trimmed[:len(trimmed)-1]
	if unit == 'd' {
		var days int64
		if _, err := fmt.Sscanf(number, "%d", &days); err != nil || fmt.Sprintf("%d", days) != number {
			return 0, fmt.Errorf("invalid --older-than %q: want <n>d with integer days", raw)
		}
		if days < 0 {
			return 0, fmt.Errorf("invalid --older-than %q: duration must not be negative", raw)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	dur, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("invalid --older-than %q: %v (e.g. 90d, 12h, 30m)", raw, err)
	}
	if dur < 0 {
		return 0, fmt.Errorf("invalid --older-than %q: duration must not be negative", raw)
	}
	return dur, nil
}

func cmdGC(args []string, stdout, stderr io.Writer) int {
	f, err := parseGCFlags(args)
	if err != nil {
		if err.Error() == "help" {
			fmt.Fprint(stdout, gcUsage)
			return exitOK
		}
		fmt.Fprintf(stderr, "proceed gc: %v\n%s", err, gcUsage)
		return exitUsage
	}
	var keep int
	if _, err := fmt.Sscanf(strings.TrimSpace(f.keepRuns), "%d", &keep); err != nil || fmt.Sprintf("%d", keep) != strings.TrimSpace(f.keepRuns) {
		fmt.Fprintf(stderr, "proceed gc: invalid --keep-runs %q: want a non-negative integer\n%s", f.keepRuns, gcUsage)
		return exitUsage
	}
	if keep < 0 {
		fmt.Fprintf(stderr, "proceed gc: invalid --keep-runs %q: want a non-negative integer\n%s", f.keepRuns, gcUsage)
		return exitUsage
	}
	older, err := parseGCDuration(f.olderThan)
	if err != nil {
		fmt.Fprintf(stderr, "proceed gc: %v\n%s", err, gcUsage)
		return exitUsage
	}
	if keep == 0 && !f.dryRun && !f.yes {
		fmt.Fprintf(stderr, "proceed gc: --keep-runs 0 deletes every gated terminal run; re-run with --yes to confirm\n%s", gcUsage)
		return exitUsage
	}
	cfg, err := resolveConfig(cliFlags{configPath: f.configPath, dataDir: f.dataDir})
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	st, err := store.Open(filepath.Join(cfg.DataDir, "proceed.db"))
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()
	policy := store.GCPolicy{KeepRuns: keep, OlderThan: older, OlderThanS: strings.TrimSpace(f.olderThan), DryRun: f.dryRun}
	result, err := st.GCRuns(context.Background(), policy, time.Now())
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	mode := "deleted"
	displayRuns := result.DeletedRuns
	if f.dryRun {
		mode = "would delete"
		displayRuns = result.Counts["graph_run"]
	}
	encoded, err := json.Marshal(gcCountsJSON(result.Counts))
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	fmt.Fprintf(stdout, "gc %s: runs=%d tables=%s record=%s\n", mode, displayRuns, string(encoded), result.RecordID)
	fmt.Fprintf(stderr, "proceed gc: keep-runs=%d older-than=%s dry-run=%t deleted-runs=%d anomalies=%d record=%s\n",
		keep, strings.TrimSpace(f.olderThan), f.dryRun, result.DeletedRuns, result.Anomalies, result.RecordID)
	return exitOK
}

func gcCountsJSON(counts map[string]int) map[string]int {
	out := map[string]int{}
	tables := make([]string, 0, len(counts))
	for table := range counts {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		out[table] = counts[table]
	}
	return out
}

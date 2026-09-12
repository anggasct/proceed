package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"proceed/internal/store"
)

const dashboardUsage = `usage:
  proceed dashboard [--data-dir <dir>] [--config <file>]
`

const dashboardTickInterval = 5 * time.Second

const dashboardClearScreen = "\x1b[H\x1b[2J"

type dashboardKey int

const (
	dashboardKeyQuit dashboardKey = iota
	dashboardKeyRefresh
	dashboardKeyUp
	dashboardKeyDown
)

type dashboardLoopDeps struct {
	query func(ctx context.Context) (*store.DashboardSnapshot, error)
	out   io.Writer
	keys  <-chan dashboardKey
	ticks <-chan time.Time
	done  <-chan struct{}
	width func() int
	now   func() time.Time
}

func cmdDashboard(args []string, stdout, stderr io.Writer) int {
	flags, positional, err := parseCommonFlags(args)
	if err != nil || len(positional) != 0 {
		fmt.Fprint(stderr, dashboardUsage)
		return exitUsage
	}
	cfg, err := resolveConfig(flags)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if !isDashboardTTY(stdout) {
		fmt.Fprintln(stderr, "proceed dashboard: stdout is not a terminal; refusing to emit control codes into a pipe")
		return exitUsage
	}
	st, err := store.Open(cfg.DataDir + "/proceed.db")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()

	restore := func() {}
	if isDashboardTerminalStdin(os.Stdin) {
		restore, err = setDashboardRawMode(os.Stdin)
		if err != nil {
			fmt.Fprintf(stderr, "proceed dashboard: %v\n", err)
			return exitUnclassified
		}
	}
	defer restore()

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ticker := time.NewTicker(dashboardTickInterval)
	defer ticker.Stop()
	return runDashboardLoop(dashboardLoopDeps{
		query: st.DashboardSnapshot,
		out:   stdout,
		keys:  pumpDashboardKeys(os.Stdin),
		ticks: ticker.C,
		done:  sigCtx.Done(),
		width: func() int { return dashboardWidth(stdout) },
		now:   time.Now,
	})
}

func runDashboardLoop(deps dashboardLoopDeps) int {
	var snap *store.DashboardSnapshot
	cursor := 0
	stale := false
	refresh := func() {
		if fresh, err := deps.query(context.Background()); err == nil {
			snap = fresh
			stale = false
		} else {
			stale = true
		}
		cursor = clampDashboardCursor(cursor, dashboardRowCount(snap))
		frame := renderDashboardFrame(snap, cursor, deps.width(), deps.now(), stale)
		_, _ = io.WriteString(deps.out, dashboardClearScreen+frame)
	}
	refresh()
	for {
		select {
		case <-deps.done:
			return exitOK
		case key, ok := <-deps.keys:
			if !ok {
				return exitOK
			}
			switch key {
			case dashboardKeyQuit:
				return exitOK
			case dashboardKeyRefresh:
				refresh()
			case dashboardKeyUp:
				if cursor > 0 {
					cursor--
					frame := renderDashboardFrame(snap, cursor, deps.width(), deps.now(), stale)
					_, _ = io.WriteString(deps.out, dashboardClearScreen+frame)
				}
			case dashboardKeyDown:
				if cursor+1 < dashboardRowCount(snap) {
					cursor++
					frame := renderDashboardFrame(snap, cursor, deps.width(), deps.now(), stale)
					_, _ = io.WriteString(deps.out, dashboardClearScreen+frame)
				}
			}
		case <-deps.ticks:
			refresh()
		}
	}
}

func pumpDashboardKeys(r io.Reader) <-chan dashboardKey {
	out := make(chan dashboardKey, 16)
	go func() {
		defer close(out)
		const (
			stateText = iota
			stateESC
			stateCSI
		)
		state := stateText
		buf := make([]byte, 32)
		emit := func(k dashboardKey) {
			if k == dashboardKeyQuit {
				out <- k
				return
			}
			select {
			case out <- k:
			default:
			}
		}
		for {
			n, err := r.Read(buf)
			if err != nil || n == 0 {
				emit(dashboardKeyQuit)
				return
			}
			for _, b := range buf[:n] {
				switch state {
				case stateText:
					switch b {
					case 'q', 'Q', 0x03:
						emit(dashboardKeyQuit)
					case 'r', 'R':
						emit(dashboardKeyRefresh)
					case 0x1b:
						state = stateESC
					}
				case stateESC:
					switch b {
					case '[':
						state = stateCSI
					case 'O':
						state = stateCSI
					case 0x1b:
						state = stateESC
					default:
						state = stateText
					}
				case stateCSI:
					switch b {
					case 'A':
						emit(dashboardKeyUp)
					case 'B':
						emit(dashboardKeyDown)
					}
					state = stateText
				}
			}
			if state == stateESC {
				emit(dashboardKeyQuit)
				state = stateText
			}
		}
	}()
	return out
}

type dashboardRow struct {
	text string
}

func dashboardRows(snap *store.DashboardSnapshot) []dashboardRow {
	if snap == nil {
		return nil
	}
	var rows []dashboardRow
	for _, r := range snap.Runs {
		rows = append(rows, dashboardRow{text: fmt.Sprintf("%s %s %s", shortDashboardID(r.RunID), r.GraphName, r.Status)})
	}
	for _, a := range snap.Approvals {
		rows = append(rows, dashboardRow{text: fmt.Sprintf("approval %s run %s node %s scope %s",
			shortDashboardID(a.ID), shortDashboardID(a.RunID), a.NodeKey, a.Scope)})
	}
	for _, w := range snap.Waits {
		rows = append(rows, dashboardRow{text: fmt.Sprintf("wait %s run %s node %s %s %s",
			shortDashboardID(w.ID), shortDashboardID(w.RunID), w.NodeKey, w.EventType, w.CorrelationKey)})
	}
	for _, r := range snap.FailedRuns {
		rows = append(rows, dashboardRow{text: fmt.Sprintf("failed run %s %s", shortDashboardID(r.RunID), r.GraphName)})
	}
	for _, n := range snap.FailedNodes {
		rows = append(rows, dashboardRow{text: fmt.Sprintf("failed node %s run %s", n.NodeKey, shortDashboardID(n.RunID))})
	}
	return rows
}

func dashboardRowCount(snap *store.DashboardSnapshot) int {
	return len(dashboardRows(snap))
}

func clampDashboardCursor(cursor, count int) int {
	if count <= 0 {
		return 0
	}
	if cursor < 0 {
		return 0
	}
	if cursor >= count {
		return count - 1
	}
	return cursor
}

func renderDashboardFrame(snap *store.DashboardSnapshot, cursor, width int, now time.Time, stale bool) string {
	if width < 1 {
		width = 80
	}
	var b strings.Builder
	status := fmt.Sprintf("proceed dashboard — %s · q quit · r refresh · up/down select", now.Format("15:04:05"))
	if stale {
		status += " · stale, retrying"
	}
	b.WriteString(truncateDashboardLine(status, width) + "\n")
	if snap == nil {
		b.WriteString(truncateDashboardLine("store unreachable", width) + "\n")
		return b.String()
	}
	rows := dashboardRows(snap)
	rowIndex := 0
	writeLines := func(count int) {
		for i := 0; i < count; i++ {
			marker := "  "
			if rowIndex == cursor {
				marker = "> "
			}
			b.WriteString(truncateDashboardLine(marker+rows[rowIndex].text, width) + "\n")
			rowIndex++
		}
	}
	pendingShown := len(snap.Approvals) + len(snap.Waits)
	pendingTotal := snap.TotalApprovals + snap.TotalWaits
	failuresShown := len(snap.FailedRuns) + len(snap.FailedNodes)
	failuresTotal := snap.TotalFailedRuns + snap.TotalFailedNodes

	b.WriteString(truncateDashboardLine(fmt.Sprintf("RUNS (%d of %d)", len(snap.Runs), snap.TotalRuns), width) + "\n")
	if len(snap.Runs) == 0 {
		b.WriteString(truncateDashboardLine("  (no runs)", width) + "\n")
	} else {
		writeLines(len(snap.Runs))
	}
	b.WriteString(truncateDashboardLine(fmt.Sprintf("PENDING (%d of %d)", pendingShown, pendingTotal), width) + "\n")
	if pendingShown == 0 {
		b.WriteString(truncateDashboardLine("  (no pending items)", width) + "\n")
	} else {
		writeLines(len(snap.Approvals) + len(snap.Waits))
	}
	b.WriteString(truncateDashboardLine(fmt.Sprintf("FAILURES (%d of %d)", failuresShown, failuresTotal), width) + "\n")
	if failuresShown == 0 {
		b.WriteString(truncateDashboardLine("  (no failures)", width) + "\n")
	} else {
		writeLines(len(snap.FailedRuns) + len(snap.FailedNodes))
	}
	return b.String()
}

func shortDashboardID(id string) string {
	id = sanitizeDashboardField(id)
	if utf8.RuneCountInString(id) > 8 {
		n := 0
		for i := range id {
			if n == 8 {
				return id[:i]
			}
			n++
		}
	}
	return id
}

func sanitizeDashboardField(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s)
}

func truncateDashboardLine(s string, width int) string {
	s = sanitizeDashboardField(s)
	if utf8.RuneCountInString(s) <= width {
		return s
	}
	n := 0
	for i := range s {
		if n == width {
			return s[:i]
		}
		n++
	}
	return s
}

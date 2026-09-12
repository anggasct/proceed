package controller

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"proceed/internal/compiler"
)

const cronRule = "E201"

type CronSchedule struct {
	minutes uint64
	hours   uint64
	dom     uint64
	months  uint64
	dow     uint64
	domStar bool
	dowStar bool
}

func ParseCron(expr string) (*CronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, cronInvalid("expression must have 5 fields (minute hour dom month dow)")
	}
	s := &CronSchedule{}
	var err error
	if s.minutes, _, err = parseCronField(fields[0], 0, 59, false); err != nil {
		return nil, err
	}
	if s.hours, _, err = parseCronField(fields[1], 0, 23, false); err != nil {
		return nil, err
	}
	if s.dom, s.domStar, err = parseCronField(fields[2], 1, 31, false); err != nil {
		return nil, err
	}
	if s.months, _, err = parseCronField(fields[3], 1, 12, false); err != nil {
		return nil, err
	}
	if s.dow, s.dowStar, err = parseCronField(fields[4], 0, 7, true); err != nil {
		return nil, err
	}
	return s, nil
}

func parseCronField(field string, lo, hi int, foldSeven bool) (uint64, bool, error) {
	star := field == "*"
	var bits uint64
	for _, part := range strings.Split(field, ",") {
		if part == "" {
			return 0, false, cronInvalid("empty list element in %q", field)
		}
		rangePart, stepPart, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			v, err := strconv.Atoi(stepPart)
			if err != nil || v < 1 {
				return 0, false, cronInvalid("invalid step in %q", part)
			}
			step = v
		}
		start, end := lo, hi
		switch {
		case rangePart == "*" || rangePart == "":
			if rangePart == "" {
				return 0, false, cronInvalid("empty range in %q", part)
			}
		default:
			l, r, hasRange := strings.Cut(rangePart, "-")
			a, err := strconv.Atoi(l)
			if err != nil || a < lo || a > hi {
				return 0, false, cronInvalid("value %q out of range %d-%d", l, lo, hi)
			}
			start = a
			end = a
			if hasRange {
				b, err := strconv.Atoi(r)
				if err != nil || b < lo || b > hi || b < a {
					return 0, false, cronInvalid("range %q invalid for %d-%d", rangePart, lo, hi)
				}
				end = b
			}
		}
		for v := start; v <= end; v += step {
			bit := v
			if foldSeven && bit == 7 {
				bit = 0
			}
			bits |= 1 << bit
		}
	}
	return bits, star, nil
}

func cronInvalid(format string, args ...any) error {
	return &compiler.Error{
		Code: compiler.CodeGraphInvalid,
		Diagnostics: []compiler.Diagnostic{{
			Rule:     cronRule,
			Location: "cron",
			Message:  fmt.Sprintf(format, args...),
		}},
	}
}

func (s *CronSchedule) Matches(t time.Time) bool {
	u := t.UTC()
	if s.minutes&(1<<uint(u.Minute())) == 0 {
		return false
	}
	if s.hours&(1<<uint(u.Hour())) == 0 {
		return false
	}
	if s.months&(1<<uint(u.Month())) == 0 {
		return false
	}
	domMatch := s.dom&(1<<uint(u.Day())) != 0
	dowMatch := s.dow&(1<<uint(int(u.Weekday()))) != 0
	if !s.domStar && !s.dowStar {
		return domMatch || dowMatch
	}
	return domMatch && dowMatch
}

const cronSearchBound = 4 * 366 * 24 * 60

func (s *CronSchedule) Next(after time.Time) (time.Time, bool) {
	cur := after.UTC().Truncate(time.Minute).Add(time.Minute)
	for i := 0; i < cronSearchBound; i++ {
		if s.Matches(cur) {
			return cur, true
		}
		cur = cur.Add(time.Minute)
	}
	return time.Time{}, false
}

func (c *Controller) FireDueSchedules(ctx context.Context, now time.Time) error {
	outcomes, err := c.store.FireDueSchedules(ctx, now.UnixMilli(), func(expr string, after time.Time) (time.Time, bool, error) {
		sched, err := ParseCron(expr)
		if err != nil {
			return time.Time{}, false, err
		}
		next, ok := sched.Next(after)
		if !ok {
			return time.Time{}, false, cronInvalid("expression %q never matches", expr)
		}
		return next, true, nil
	})
	if err != nil {
		return err
	}
	for _, o := range outcomes {
		if o.RunID != "" {
			runID := o.RunID
			log.Printf("schedule %s fired run %s", o.Name, runID)
			go func() {
				drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()
				_ = c.Drain(drainCtx, runID)
			}()
		} else if o.Skipped > 0 {
			log.Printf("schedule %s skipped %d ticks (window %d..%d)", o.Name, o.Skipped, o.SkipFrom, o.SkipTo)
		}
	}
	return nil
}

func CronNeverMatches(expr string) error {
	return cronInvalid("expression %q never matches", expr)
}

package main

import (
	"context"
	"fmt"
	"io"
	"os/user"
	"strings"

	"github.com/oklog/ulid/v2"

	"proceed/internal/controller"
	"proceed/internal/store"
)

const approveUsage = `usage:
  proceed approve <run-id> <approval-id> --decision grant|deny [--actor <name>] [--idempotency-key <key>] [--reason <note>] [--data-dir <dir>] [--config <file>]
`

func cmdApprove(args []string, stdout, stderr io.Writer) int {
	flags := cliFlags{}
	var decision, actor, idempotencyKey, reason string
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		var value string
		hasValue := false
		if strings.HasPrefix(arg, "--") && strings.Contains(arg, "=") {
			eq := strings.IndexByte(arg, '=')
			value = arg[eq+1:]
			arg = arg[:eq]
			hasValue = true
		}
		needsValue := func() (string, bool) {
			if hasValue {
				return value, true
			}
			if i+1 >= len(args) {
				return "", false
			}
			i++
			return args[i], true
		}
		switch arg {
		case "--decision":
			v, ok := needsValue()
			if !ok {
				fmt.Fprintln(stderr, "proceed: --decision requires a value")
				return exitUsage
			}
			decision = v
		case "--actor":
			v, ok := needsValue()
			if !ok {
				fmt.Fprintln(stderr, "proceed: --actor requires a value")
				return exitUsage
			}
			actor = v
		case "--idempotency-key":
			v, ok := needsValue()
			if !ok {
				fmt.Fprintln(stderr, "proceed: --idempotency-key requires a value")
				return exitUsage
			}
			idempotencyKey = v
		case "--reason":
			v, ok := needsValue()
			if !ok {
				fmt.Fprintln(stderr, "proceed: --reason requires a value")
				return exitUsage
			}
			reason = v
		case "--config", "--data-dir", "--bind":
			v, ok := needsValue()
			if !ok {
				fmt.Fprintf(stderr, "proceed: %s requires a value\n", arg)
				return exitUsage
			}
			switch arg {
			case "--config":
				flags.configPath = v
			case "--data-dir":
				flags.dataDir = v
			case "--bind":
				flags.bind = v
			}
		default:
			positional = append(positional, args[i])
		}
	}
	if len(positional) != 2 {
		fmt.Fprint(stderr, approveUsage)
		return exitUsage
	}
	runID, approvalID := positional[0], positional[1]

	if decision != "grant" && decision != "deny" {
		fmt.Fprint(stderr, approveUsage)
		return exitUsage
	}
	if actor == "" {
		if current, err := user.Current(); err == nil && current.Username != "" {
			actor = current.Username
		} else {
			fmt.Fprintln(stderr, "proceed approve: --actor is required when the OS identity is unavailable")
			return exitUsage
		}
	}
	if idempotencyKey == "" {
		idempotencyKey = ulid.Make().String()
	}

	cfg, err := resolveConfig(flags)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	st, err := store.Open(cfg.DataDir + "/proceed.db")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()

	c, err := controller.New(st, controller.DefaultConfig(), nil)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}

	result, err := c.DecideApproval(context.Background(), controller.ApprovalDecisionRequest{
		ApprovalID:     approvalID,
		RunID:          runID,
		Decision:       decision,
		Actor:          actor,
		IdempotencyKey: idempotencyKey,
		Reason:         reason,
	})
	if err != nil {
		return printClassified(err, stderr)
	}

	switch result.Code {
	case controller.ApprovalDecided, controller.ApprovalAlreadyDecided:
		fmt.Fprintf(stdout, "approval %s %s (decision=%s actor=%s run=%s node=%s)\n",
			result.ApprovalID, result.Code, result.Decision, result.Actor, result.RunID, result.NodeKey)
		return exitOK
	case controller.ApprovalExpired:
		fmt.Fprintf(stdout, "approval %s APPROVAL_EXPIRED (run=%s node=%s)\n",
			result.ApprovalID, result.RunID, result.NodeKey)
		return exitCodeForClass("APPROVAL_EXPIRED")
	case "RUN_NOT_FOUND":
		fmt.Fprintf(stderr, "proceed approve: %s\n", result.Message)
		return exitCodeForClass("RUN_NOT_FOUND")
	case controller.ApprovalConflict:
		fmt.Fprintf(stderr, "proceed approve: %s\n", result.Message)
		return exitCodeForClass("STORE_CONFLICT")
	default:
		fmt.Fprintf(stderr, "proceed approve: %s\n", result.Message)
		return exitCodeForClass(result.Code)
	}
}

package main

import (
	"errors"

	"proceed/internal/executor"
	"proceed/internal/store"
)

const (
	exitOK           = 0
	exitUnclassified = 1
	exitUsage        = 2
)

var classExitCodes = map[string]int{
	"GRAPH_INVALID":     10,
	"POLICY_DENIED":     11,
	"RUN_NOT_FOUND":     12,
	"NODE_TIMEOUT":      13,
	"NODE_FAILED":       14,
	"EFFECT_UNCERTAIN":  15,
	"APPROVAL_REQUIRED": 16,
	"APPROVAL_EXPIRED":  17,
	"RUN_CANCELLED":     18,
	"STORE_BUSY":        19,
	"STORE_CONFLICT":    20,
}

func exitCodeForClass(class string) int {
	if code, ok := classExitCodes[class]; ok {
		return code
	}
	return exitUnclassified
}

func exitCodeForError(err error) int {
	if err == nil {
		return exitOK
	}
	if code := store.ErrorCode(err); code != "" {
		return exitCodeForClass(code)
	}
	switch {
	case errors.Is(err, executor.ErrTimeout):
		return exitCodeForClass("NODE_TIMEOUT")
	case errors.Is(err, executor.ErrCancelled):
		return exitCodeForClass("RUN_CANCELLED")
	case errors.Is(err, executor.ErrUncertain):
		return exitCodeForClass("EFFECT_UNCERTAIN")
	}
	text := err.Error()
	for class := range classExitCodes {
		if textContainsClass(text, class) {
			return exitCodeForClass(class)
		}
	}
	return exitUnclassified
}

func textContainsClass(text, class string) bool {
	for i := 0; i+len(class) <= len(text); i++ {
		if text[i:i+len(class)] == class {
			if i > 0 && isWordByte(text[i-1]) {
				continue
			}
			if i+len(class) < len(text) && isWordByte(text[i+len(class)]) {
				continue
			}
			return true
		}
	}
	return false
}

func isWordByte(b byte) bool {
	return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}

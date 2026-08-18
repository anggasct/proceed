package controller

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
	"proceed/internal/store"
)

type Config struct {
	LeaseTTL        time.Duration
	HeartbeatPeriod time.Duration
	MaxConcurrent   int
	OwnerID         string
	Mode            string
}

func DefaultConfig() Config {
	return Config{
		LeaseTTL:        30 * time.Second,
		HeartbeatPeriod: 10 * time.Second,
		MaxConcurrent:   8,
		OwnerID:         "controller-" + ulid.Make().String(),
		Mode:            "run",
	}
}

type Controller struct {
	store *store.Store
	cfg   Config
	pool  map[executor.Kind]executor.Executor

	inflightMu sync.Mutex
	inflight   map[string]inflightExecution
}

type inflightExecution struct {
	cancelContext context.CancelFunc
	cancelRequest func()
}

func (c *Controller) trackInflight(runID, nodeKey string, execution inflightExecution) bool {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()
	if c.inflight == nil {
		c.inflight = map[string]inflightExecution{}
	}
	key := inflightKey(runID, nodeKey)
	if _, exists := c.inflight[key]; exists {
		return false
	}
	c.inflight[key] = execution
	return true
}

func (c *Controller) untrackInflight(runID, nodeKey string) {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()
	delete(c.inflight, inflightKey(runID, nodeKey))
}

func (c *Controller) cancelInflightRun(runID string) {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()
	for key, execution := range c.inflight {
		if strings.HasPrefix(key, runID+"\x00") {
			execution.cancelContext()
			execution.cancelRequest()
		}
	}
}

func inflightKey(runID, nodeKey string) string {
	return runID + "\x00" + nodeKey
}

func New(st *store.Store, cfg Config, pool map[executor.Kind]executor.Executor) (*Controller, error) {
	if cfg.LeaseTTL <= 0 || cfg.HeartbeatPeriod <= 0 || cfg.MaxConcurrent <= 0 {
		return nil, fmt.Errorf("controller config requires positive lease ttl, heartbeat period, and concurrency")
	}
	if cfg.OwnerID == "" {
		cfg.OwnerID = "controller-" + ulid.Make().String()
	}
	if cfg.Mode != "run" && cfg.Mode != "serve" {
		return nil, fmt.Errorf("controller mode must be run or serve")
	}
	return &Controller{store: st, cfg: cfg, pool: pool}, nil
}

func (c *Controller) OwnerID() string { return c.cfg.OwnerID }

var errLeaseLost = errors.New("controller lease lost")

func (c *Controller) acquireLease(ctx context.Context, now time.Time) error {
	nowMs := now.UnixMilli()
	ttlMs := c.cfg.LeaseTTL.Milliseconds()
	heartbeat := nowMs
	expires := nowMs + ttlMs
	return c.store.WithTx(ctx, func(tx *sql.Tx) error {
		var ownerID string
		var expiresAt int64
		err := tx.QueryRowContext(ctx,
			"SELECT owner_id, lease_expires_at FROM controller_lease WHERE store_id = 'default'").Scan(&ownerID, &expiresAt)
		switch {
		case err == sql.ErrNoRows:
			_, err = tx.ExecContext(ctx, `
INSERT INTO controller_lease (store_id, owner_id, mode, heartbeat_at, lease_expires_at)
VALUES ('default', ?, ?, ?, ?)`,
				c.cfg.OwnerID, c.cfg.Mode, heartbeat, expires)
			return err
		case err != nil:
			return err
		case ownerID == c.cfg.OwnerID:
			_, err = tx.ExecContext(ctx, `
UPDATE controller_lease SET heartbeat_at = ?, lease_expires_at = ?, mode = ? WHERE store_id = 'default'`,
				heartbeat, expires, c.cfg.Mode)
			return err
		case expiresAt <= nowMs:
			_, err = tx.ExecContext(ctx, `
UPDATE controller_lease SET owner_id = ?, mode = ?, heartbeat_at = ?, lease_expires_at = ?
WHERE store_id = 'default' AND (owner_id = ? OR lease_expires_at <= ?)`,
				c.cfg.OwnerID, c.cfg.Mode, heartbeat, expires, ownerID, nowMs)
			if err != nil {
				return err
			}
			return nil
		default:
			return store.NewCodeError(store.CodeStoreBusy,
				"store is owned by controller %s until %d", ownerID, expiresAt)
		}
	})
}

func (c *Controller) heartbeat(ctx context.Context) error {
	nowMs := time.Now().UnixMilli()
	expires := nowMs + c.cfg.LeaseTTL.Milliseconds()
	res, err := c.store.DB().ExecContext(ctx, `
UPDATE controller_lease SET heartbeat_at = ?, lease_expires_at = ? WHERE store_id = 'default' AND owner_id = ?`,
		nowMs, expires, c.cfg.OwnerID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return errLeaseLost
	}
	return nil
}

func (c *Controller) leaseValid(ctx context.Context) error {
	var count int
	err := c.store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM controller_lease WHERE store_id = 'default' AND owner_id = ? AND lease_expires_at > ?",
		c.cfg.OwnerID, time.Now().UnixMilli()).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return errLeaseLost
	}
	return nil
}

func (c *Controller) releaseLease(ctx context.Context) {
	if _, err := c.store.DB().ExecContext(ctx,
		"DELETE FROM controller_lease WHERE store_id = 'default' AND owner_id = ?", c.cfg.OwnerID); err != nil {
		_ = err
	}
}

func OperationKey(runID, digest, nodeKey string, attempt int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%d", runID, digest, nodeKey, attempt)
	return hex.EncodeToString(h.Sum(nil))
}

func (c *Controller) appendWithin(ctx context.Context, tx *sql.Tx, ev *store.Event) (int64, error) {
	if ev.IdempotencyKey == "" {
		ev.IdempotencyKey = ulid.Make().String()
	}
	if err := c.store.AppendTx(ctx, tx, ev); err != nil {
		return 0, err
	}
	return ev.Sequence, nil
}

func payloadJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func trimErr(err error) string {
	return strings.TrimSpace(err.Error())
}

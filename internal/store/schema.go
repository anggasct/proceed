package store

const storeSchemaVersion = 2

const schemaDDL = `
CREATE TABLE IF NOT EXISTS graph (
  id   TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE COLLATE NOCASE
);

CREATE TABLE IF NOT EXISTS graph_version (
  id                      TEXT PRIMARY KEY,
  graph_id                TEXT NOT NULL REFERENCES graph(id),
  definition_digest       TEXT NOT NULL UNIQUE,
  source_schema_version   TEXT NOT NULL,
  compiled_schema_version TEXT NOT NULL,
  source_metadata         TEXT NOT NULL,
  extras                  TEXT NOT NULL DEFAULT '{}',
  status                  TEXT NOT NULL DEFAULT 'frozen'
                            CHECK (status IN ('frozen', 'superseded')),
  created_at              INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS graph_node (
  id               TEXT PRIMARY KEY,
  graph_version_id TEXT NOT NULL REFERENCES graph_version(id),
  node_key         TEXT NOT NULL,
  type             TEXT NOT NULL
                   CHECK (type IN ('task','model','agent','tool','verifier','router','gate')),
  config           TEXT NOT NULL,
  UNIQUE (graph_version_id, node_key)
);

CREATE TABLE IF NOT EXISTS graph_edge (
  id               TEXT PRIMARY KEY,
  graph_version_id TEXT NOT NULL REFERENCES graph_version(id),
  from_node_key    TEXT NOT NULL,
  to_node_key      TEXT NOT NULL,
  type             TEXT NOT NULL
                   CHECK (type IN ('defines','depends_on','routes_to','produces','consumes',
                                   'verifies','derived_from','blocks','approves','measures','improves')),
  condition        TEXT,
  max_traversals   INTEGER,
  extras           TEXT NOT NULL DEFAULT '{}',
  CHECK (type = 'routes_to' OR condition IS NULL),
  FOREIGN KEY (graph_version_id, from_node_key)
    REFERENCES graph_node(graph_version_id, node_key),
  FOREIGN KEY (graph_version_id, to_node_key)
    REFERENCES graph_node(graph_version_id, node_key)
);

CREATE TABLE IF NOT EXISTS policy (
  id               TEXT PRIMARY KEY,
  graph_version_id TEXT NOT NULL REFERENCES graph_version(id),
  kind             TEXT NOT NULL,
  rule             TEXT NOT NULL,
  extras           TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS graph_run (
  id                TEXT PRIMARY KEY,
  graph_version_id  TEXT NOT NULL REFERENCES graph_version(id),
  definition_digest TEXT NOT NULL,
  status            TEXT NOT NULL
                    CHECK (status IN ('running','completed','failed','cancelled','abandoned')),
  created_at        INTEGER NOT NULL,
  started_at        INTEGER,
  finished_at       INTEGER
);

CREATE TABLE IF NOT EXISTS run_node (
  id            TEXT PRIMARY KEY,
  run_id        TEXT NOT NULL REFERENCES graph_run(id),
  node_key      TEXT NOT NULL,
  status        TEXT NOT NULL
                CHECK (status IN ('pending','eligible','leased','running','succeeded','failed',
                                  'uncertain','waiting','reconciling','cancel_requested',
                                  'cancelled','skipped')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  started_at    INTEGER,
  finished_at   INTEGER,
  UNIQUE (run_id, node_key)
);

CREATE TABLE IF NOT EXISTS run_edge (
  id              TEXT PRIMARY KEY,
  run_id          TEXT NOT NULL REFERENCES graph_run(id),
  edge_id         TEXT NOT NULL REFERENCES graph_edge(id),
  route           TEXT,
  sequence_in_run INTEGER NOT NULL,
  traversed_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS node_attempt (
  id                   TEXT PRIMARY KEY,
  run_node_id          TEXT NOT NULL REFERENCES run_node(id),
  attempt_no           INTEGER NOT NULL,
  operation_key        TEXT NOT NULL UNIQUE,
  executor             TEXT NOT NULL
                       CHECK (executor IN ('shell','http','human_approval','agent_cli')),
  side_effect_contract TEXT NOT NULL
                       CHECK (side_effect_contract IN ('pure','idempotent','reconcilable','non_replayable')),
  lease_token          TEXT,
  lease_expires_at     INTEGER,
  status               TEXT NOT NULL
                       CHECK (status IN ('leased','running','succeeded','failed','uncertain','cancelled')),
  result               TEXT,
  started_at           INTEGER,
  finished_at          INTEGER,
  UNIQUE (run_node_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS controller_lease (
  store_id         TEXT PRIMARY KEY CHECK (store_id = 'default'),
  owner_id         TEXT NOT NULL,
  mode             TEXT NOT NULL CHECK (mode IN ('run','serve')),
  heartbeat_at     INTEGER NOT NULL,
  lease_expires_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS event (
  event_id        TEXT PRIMARY KEY,
  run_id          TEXT NOT NULL REFERENCES graph_run(id),
  sequence        INTEGER NOT NULL,
  schema_version  TEXT NOT NULL,
  type            TEXT NOT NULL,
  occurred_at     INTEGER NOT NULL,
  recorded_at     INTEGER NOT NULL,
  actor_type      TEXT NOT NULL
                  CHECK (actor_type IN ('controller','executor','node','human')),
  actor_id        TEXT NOT NULL,
  causation_id    TEXT REFERENCES event(event_id),
  correlation_id  TEXT,
  idempotency_key TEXT UNIQUE,
  payload_digest  TEXT NOT NULL,
  payload         TEXT NOT NULL,
  UNIQUE (run_id, sequence),
  CHECK (occurred_at <= recorded_at)
);

CREATE TABLE IF NOT EXISTS artifact (
  id                    TEXT PRIMARY KEY,
  run_id                TEXT NOT NULL REFERENCES graph_run(id),
  produced_by_node_key  TEXT NOT NULL,
  name                  TEXT NOT NULL,
  path                  TEXT NOT NULL,
  content_hash          TEXT NOT NULL,
  media_type            TEXT NOT NULL,
  size_bytes            INTEGER NOT NULL CHECK (size_bytes >= 0),
  truncated             INTEGER NOT NULL DEFAULT 0,
  created_at            INTEGER NOT NULL,
  UNIQUE (run_id, produced_by_node_key, name, content_hash)
);

CREATE TABLE IF NOT EXISTS evaluation (
  id                    TEXT PRIMARY KEY,
  artifact_id           TEXT NOT NULL REFERENCES artifact(id),
  evaluated_by_node_key TEXT NOT NULL,
  run_id                TEXT NOT NULL REFERENCES graph_run(id),
  verdict               TEXT NOT NULL CHECK (verdict IN ('passed','failed')),
  evidence_ref          TEXT,
  evaluated_at          INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS effect (
  id                 TEXT PRIMARY KEY,
  node_attempt_id    TEXT NOT NULL REFERENCES node_attempt(id),
  operation_key      TEXT NOT NULL,
  target             TEXT NOT NULL,
  status             TEXT NOT NULL
                     CHECK (status IN ('pending','confirmed','rejected','unknown')),
  request_digest     TEXT NOT NULL,
  receipt            TEXT,
  reconciliation_ref TEXT,
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS decision (
  id                 TEXT PRIMARY KEY,
  run_id             TEXT NOT NULL REFERENCES graph_run(id),
  run_node_id        TEXT NOT NULL REFERENCES run_node(id),
  kind               TEXT NOT NULL CHECK (kind IN ('routing','gate','retry')),
  candidate_edges    TEXT NOT NULL,
  selected_edge_id   TEXT REFERENCES graph_edge(id),
  rejection          TEXT,
  predicate_snapshot TEXT NOT NULL,
  input_references   TEXT NOT NULL,
  policy_version     TEXT NOT NULL,
  decided_at         INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS causal_link (
  id                 TEXT PRIMARY KEY,
  decision_id        TEXT NOT NULL REFERENCES decision(id),
  target_run_node_id TEXT NOT NULL REFERENCES run_node(id),
  attribution        TEXT NOT NULL
                     CHECK (attribution IN ('necessary','sufficient','contributing',
                                            'blocked_by','unknown')),
  source_kind        TEXT NOT NULL
                     CHECK (source_kind IN ('decision','evaluation','approval','artifact','event')),
  source_id          TEXT NOT NULL,
  citation_type      TEXT CHECK (citation_type IN ('artifact','evaluation','approval','event')),
  citation_id        TEXT,
  group_key          TEXT,
  CHECK ((citation_type IS NULL) = (citation_id IS NULL))
);

CREATE TABLE IF NOT EXISTS approval (
  id                       TEXT PRIMARY KEY,
  run_id                   TEXT NOT NULL REFERENCES graph_run(id),
  run_node_id              TEXT NOT NULL REFERENCES run_node(id),
  graph_version_id         TEXT NOT NULL REFERENCES graph_version(id),
  requested_action         TEXT NOT NULL,
  evidence_references      TEXT NOT NULL,
  required_scope           TEXT NOT NULL,
  expires_at               INTEGER NOT NULL,
  decision                 TEXT CHECK (decision IN ('grant','deny')),
  decided_by               TEXT,
  decided_at               INTEGER,
  decision_idempotency_key TEXT UNIQUE,
  created_at               INTEGER NOT NULL,
  CHECK ((decision IS NULL) = (decided_by IS NULL)),
  CHECK ((decision IS NULL) = (decided_at IS NULL))
);

CREATE TABLE IF NOT EXISTS anchor (
  id               TEXT PRIMARY KEY,
  graph_version_id TEXT NOT NULL REFERENCES graph_version(id),
  created_at       INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS outcome (
  id          TEXT PRIMARY KEY,
  run_id      TEXT NOT NULL UNIQUE REFERENCES graph_run(id),
  anchor_id   TEXT NOT NULL UNIQUE REFERENCES anchor(id),
  result      TEXT NOT NULL
              CHECK (result IN ('completed','failed','cancelled','abandoned')),
  detail      TEXT,
  recorded_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS metric (
  id          TEXT PRIMARY KEY,
  anchor_id   TEXT NOT NULL REFERENCES anchor(id),
  name        TEXT NOT NULL,
  value       REAL NOT NULL,
  unit        TEXT NOT NULL,
  dimensions  TEXT,
  recorded_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_change_proposal (
  id                      TEXT PRIMARY KEY,
  target_graph_version_id TEXT NOT NULL REFERENCES graph_version(id),
  status                  TEXT NOT NULL
                          CHECK (status IN ('draft','proposed','approved','rejected','superseded')),
  rationale               TEXT NOT NULL,
  proposed_change         TEXT NOT NULL,
  approval_id             TEXT REFERENCES approval(id),
  supersedes_proposal_id  TEXT REFERENCES policy_change_proposal(id),
  created_at              INTEGER NOT NULL,
  decided_at              INTEGER,
  CHECK (status <> 'approved' OR approval_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS proposal_metric (
  proposal_id TEXT NOT NULL REFERENCES policy_change_proposal(id),
  metric_id   TEXT NOT NULL REFERENCES metric(id),
  PRIMARY KEY (proposal_id, metric_id)
);

CREATE INDEX IF NOT EXISTS idx_graph_node_version ON graph_node(graph_version_id);
CREATE INDEX IF NOT EXISTS idx_graph_edge_version ON graph_edge(graph_version_id);
CREATE INDEX IF NOT EXISTS idx_policy_version ON policy(graph_version_id);
CREATE INDEX IF NOT EXISTS idx_run_node_run        ON run_node(run_id);
CREATE INDEX IF NOT EXISTS idx_run_edge_run        ON run_edge(run_id);
CREATE INDEX IF NOT EXISTS idx_run_edge_traversals ON run_edge(run_id, edge_id);
CREATE INDEX IF NOT EXISTS idx_node_attempt_lease  ON node_attempt(lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_event_run           ON event(run_id, sequence);
CREATE INDEX IF NOT EXISTS idx_event_causation     ON event(causation_id);
CREATE INDEX IF NOT EXISTS idx_artifact_hash       ON artifact(content_hash);
CREATE INDEX IF NOT EXISTS idx_evaluation_run      ON evaluation(run_id);
CREATE INDEX IF NOT EXISTS idx_effect_status       ON effect(status);
CREATE INDEX IF NOT EXISTS idx_effect_attempt      ON effect(node_attempt_id);
CREATE INDEX IF NOT EXISTS idx_decision_run        ON decision(run_id);
CREATE INDEX IF NOT EXISTS idx_decision_node       ON decision(run_node_id);
CREATE INDEX IF NOT EXISTS idx_causal_target       ON causal_link(target_run_node_id);
CREATE INDEX IF NOT EXISTS idx_causal_decision     ON causal_link(decision_id);
CREATE INDEX IF NOT EXISTS idx_causal_citation     ON causal_link(citation_type, citation_id);
CREATE INDEX IF NOT EXISTS idx_approval_pending    ON approval(run_id, decision, expires_at);
CREATE INDEX IF NOT EXISTS idx_metric_anchor       ON metric(anchor_id);
CREATE INDEX IF NOT EXISTS idx_proposal_target     ON policy_change_proposal(target_graph_version_id, status);
`

func (s *Store) SchemaVersion() int {
	return storeSchemaVersion
}

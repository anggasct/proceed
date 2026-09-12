package store

import (
	"context"
	"database/sql"

	"proceed/internal/compiler"
)

type RunParamValue struct {
	Name  string  `json:"name"`
	Type  string  `json:"type"`
	Value *string `json:"value,omitempty"`
}

type RunParamsStart struct {
	Digest string
	Values []RunParamValue
}

type RunParamRow struct {
	Name  string
	Type  string
	Value sql.NullString
}

func (s *Store) GraphParams(ctx context.Context, versionID string) ([]compiler.Param, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT name, type, required, default_value FROM graph_param
WHERE graph_version_id = ? ORDER BY name`, versionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []compiler.Param{}
	for rows.Next() {
		var p compiler.Param
		var required int
		var def sql.NullString
		if err := rows.Scan(&p.Name, &p.Type, &required, &def); err != nil {
			return nil, err
		}
		p.Required = required == 1
		if def.Valid {
			p.HasDefault = true
			p.Default = def.String
			p.DefaultTag = defaultTagFor(p.Type)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func defaultTagFor(typ string) string {
	switch typ {
	case "string", "secret":
		return "!!str"
	case "int":
		return "!!int"
	case "float":
		return "!!float"
	case "bool":
		return "!!bool"
	}
	return ""
}

func (s *Store) RunParams(ctx context.Context, runID string) (map[string]RunParamRow, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT name, type, value FROM run_param WHERE run_id = ? ORDER BY name", runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]RunParamRow{}
	for rows.Next() {
		var r RunParamRow
		if err := rows.Scan(&r.Name, &r.Type, &r.Value); err != nil {
			return nil, err
		}
		out[r.Name] = r
	}
	return out, rows.Err()
}

func insertGraphParams(ctx context.Context, tx *sql.Tx, versionID string, doc *compiler.Document) error {
	for i := range doc.Params {
		pa := &doc.Params[i]
		var required int
		if pa.Required {
			required = 1
		}
		var def any
		if pa.HasDefault {
			def = pa.Default
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO graph_param (graph_version_id, name, type, required, default_value)
VALUES (?, ?, ?, ?, ?)`, versionID, pa.Name, pa.Type, required, def); err != nil {
			return err
		}
	}
	return nil
}

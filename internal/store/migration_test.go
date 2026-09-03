package store

import (
	"database/sql"
	"os"
	"testing"
)

func TestOpenAddsMissingRejectionReasonColumn(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/proceed.db"
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`ALTER TABLE policy_change_proposal DROP COLUMN rejection_reason`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("PRAGMA user_version = 2"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	backup := migrationBackupPath(path)
	if err := os.Remove(backup); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	var v int
	if err := s2.db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != storeSchemaVersion {
		t.Errorf("user_version = %d, want %d", v, storeSchemaVersion)
	}

	var reason *string
	if err := s2.db.QueryRow(`SELECT rejection_reason FROM policy_change_proposal LIMIT 1`).Scan(&reason); err != nil && err != sql.ErrNoRows {
		t.Fatalf("rejection_reason column missing after migration: %v", err)
	}
}

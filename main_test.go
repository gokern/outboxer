package outboxer_test

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"
)

// This file decides whether the suite may run at all, and on what.
//
// Two things live here and nowhere else: the reference schema, embedded from the
// migration so a test cannot drift from the contract it checks, and the refusal
// to start without a database. Everything after this file assumes both.
//
// It is also the one place with a package-scope variable the whole suite reads,
// the DSN, because TestMain is the only code that runs before any test.

// referenceDDL is the schema this package documents, embedded and not copied: a
// test cannot drift from the contract it is checking if there is only
// one copy of it. Renaming it for a differently-named table is a plain string
// substitution, because every identifier the file declares is derived from the
// table name.
//
//go:embed migration/0001_create_outbox.up.sql
var referenceDDL string

// referenceTable is the name the reference DDL creates, and the name both
// constructors default to when no WithTable is given. Every uniquely-named test
// table is this one with the identifiers substituted.
const referenceTable = "outbox"

// testDSN is POSTGRES_URL, read once in TestMain.
var testDSN string

// TestMain refuses to run without a database, and ends the run on a leaked
// goroutine.
//
// Refusing, not skipping: skipping lets `go test ./...` go green with the
// replica safety that is this package's whole reason to exist never once
// executed, and a suite that goes green by skipping reports coverage it does
// not have.
//
// The leak barrier is not decoration on a package like this one. Run owns
// several goroutines, hands two of them to caller code, and its whole shutdown
// contract is a statement about goroutines outliving it. ErrShutdownIncomplete
// exists to say "publishes are still running detached". Nothing in the
// suite could observe a relay that returned cleanly and left one behind; the
// barrier fails the package when it does. It already caught one: a stalled
// publish that slept a flat thirty seconds outlived not just its test but the
// whole run.
//
// Setup closes its own pool before the barrier is armed, because pgx keeps
// goroutines behind an open pool and every one of them would be reported as a
// leak.
func TestMain(m *testing.M) {
	mustSetUp()

	goleak.VerifyTestMain(m)
}

// mustSetUp reads the DSN and puts the reference schema in place, ending the
// process with a usable message if it cannot. The pool it opens is its own: no
// test draws from it, so it is closed here instead of held for the run.
func mustSetUp() {
	testDSN = os.Getenv("POSTGRES_URL")
	if testDSN == "" {
		fail("POSTGRES_URL is not set.\n\n" +
			"These tests exercise a real Postgres and fail rather than skip without one.\n" +
			"Start a throwaway server and run them against it:\n\n" +
			"    mise run db\n" +
			"    mise run test\n\n" +
			"The database is used destructively: the suite drops and recreates its own\n" +
			"tables on every run, so point POSTGRES_URL at one it may own outright.\n")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		fail(fmt.Sprintf("connect to POSTGRES_URL: %v\n", err))
	}

	defer pool.Close()

	// The temp-table harness copies this one, so it has to exist first.
	err = applySchema(ctx, pool, referenceTable)
	if err != nil {
		fail(fmt.Sprintf("apply reference schema: %v\n", err))
	}
}

// fail ends the process before any test runs, which is the whole point: there
// is no test to fail yet, and returning would let the suite start against a
// database it cannot use.
//
//nolint:revive // deep-exit: refusing to run is the only thing this can do
func fail(message string) {
	fmt.Fprint(os.Stderr, message)
	os.Exit(1)
}

// schemaFor renders the reference DDL for a differently-named table.
func schemaFor(table string) string {
	return strings.ReplaceAll(referenceDDL, referenceTable, table)
}

// dropSchemaFor removes everything schemaFor created. The guards live here, in
// the test, and not in the reference DDL, which stays plain on purpose.
func dropSchemaFor(table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %[1]s CASCADE; DROP FUNCTION IF EXISTS %[1]s_notify();", table)
}

// applySchema drops and recreates one table's schema. It runs as a single
// multi-statement Exec, which pgx sends over the simple protocol because there
// are no arguments.
func applySchema(ctx context.Context, pool *pgxpool.Pool, table string) error {
	_, err := pool.Exec(ctx, dropSchemaFor(table))
	if err != nil {
		return fmt.Errorf("drop %s: %w", table, err)
	}

	_, err = pool.Exec(ctx, schemaFor(table))
	if err != nil {
		return fmt.Errorf("create %s: %w", table, err)
	}

	return nil
}

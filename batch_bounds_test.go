package outboxer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/gokern/outboxer"
)

// A LIMIT inside an IN subquery is not a batch boundary when a legal semi-join
// plan rescans it after each update/delete. Exercise changed rows, not SQL text.
func Test_BatchLimitsUnderPlannerStrategies(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"claim", "prune", "count"} {
		for _, nestedLoop := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nested_loop_%t", operation, nestedLoop), func(t *testing.T) {
				t.Parallel()

				withTable(t, 1, func(pool *pgxpool.Pool, table string) {
					for range 3 {
						id := insertRaw(t, pool, table, "batch", []byte("p"))
						if operation != "claim" {
							markPublishedAt(t, pool, table, id, 2*time.Hour)
						}
					}

					tx, err := pool.Begin(t.Context())
					require.NoError(t, err)
					defer func() {
						ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), settle)
						defer cancel()
						require.NoError(t, tx.Rollback(ctx))
					}()

					if nestedLoop {
						// These transaction-local controls select an allowed plan;
						// production must stay correct without fixing planner settings.
						_, err = tx.Exec(t.Context(), `
							SET LOCAL enable_hashjoin = off;
							SET LOCAL enable_mergejoin = off;
							SET LOCAL enable_hashagg = off;
							SET LOCAL enable_sort = off;
							SET LOCAL enable_material = off`)
						require.NoError(t, err)
					}

					args := []any{60.0, 2}
					if operation != "claim" {
						args[0] = time.Now().Add(-time.Hour)
					}
					statement := outboxer.ScanStatementsForTest(table)[operation]
					if operation == "count" {
						statement = outboxer.CountStatementsForTest(table)["batch"]
						args = []any{time.Now(), int64(3), 2}
					}
					changed, err := tx.Exec(t.Context(), statement, args...)
					require.NoError(t, err)
					require.Equal(t, int64(2), changed.RowsAffected(), "a legal plan must not exceed the batch limit")

					var untouched int
					query := "SELECT count(*) FROM " + table
					if operation == "claim" {
						query += " WHERE attempts = 0"
					}
					require.NoError(t, tx.QueryRow(t.Context(), query).Scan(&untouched))
					require.Equal(t, 1, untouched)
				})
			})
		}
	}
}

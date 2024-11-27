package driver_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	driver "github.com/jitsucom/bulker/bulkerlib/implementations/sql/redshift_driver"
	"k8s.io/apimachinery/pkg/util/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRedshiftDriver(t *testing.T) {
	testCases := []struct {
		name           string
		credentialsKey string
	}{
		{
			name:           "with AccessKeyID and cfg.SecretAccessKey",
			credentialsKey: "REDSHIFT_DATA_TEST_ENVIRONMENT_CREDENTIALS",
		},
		{
			name:           "with RoleARN",
			credentialsKey: "REDSHIFT_DATA_TEST_ENVIRONMENT_ROLE_ARN_CREDENTIALS",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configJSON, ok := os.LookupEnv(tc.credentialsKey)
			if !ok {
				if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
					t.Fatalf("%s environment variable not set", tc.credentialsKey)
				}
				t.Skipf("skipping redshift sdk driver integration test (%s) due to lack of a test environment", tc.name)
			}
			var cfg driver.RedshiftConfig
			err := json.Unmarshal([]byte(configJSON), &cfg)
			require.NoError(t, err, "it should be able to unmarshal the config")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			connector := driver.NewRedshiftConnector(cfg)
			db := sql.OpenDB(connector)
			schema := GenerateTestSchema()
			t.Cleanup(func() {
				_, err := db.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schema))
				require.NoError(t, err, "it should be able to drop the schema")
			})

			t.Run("Open", func(t *testing.T) {
				dsn := cfg.String()
				db, err := sql.Open("redshift-data", dsn)
				require.NoError(t, err, "it should be able to open the database")
				require.NoError(t, db.Ping(), "it should be able to ping the database")
				err = db.Close()
				require.NoError(t, err, "it should be able to close the database")
			})

			t.Run("Driver", func(t *testing.T) {
				driver := connector.Driver()
				require.NotNil(t, driver, "it should be able to get the driver")
				conn, err := driver.Open(cfg.String())
				require.NoError(t, err, "it should be able to open a connection")
				err = conn.Close()
				require.NoError(t, err, "it should be able to close the connection")
			})

			t.Run("Ping", func(t *testing.T) {
				require.NoError(t, db.Ping(), "it should be able to ping the database")
				require.NoError(t, db.PingContext(ctx), "it should be able to ping the database using a context")
			})

			t.Run("Exec", func(t *testing.T) {
				_, err := db.Exec(fmt.Sprintf(`CREATE SCHEMA "%s"`, schema))
				require.NoError(t, err, "it should be able to create a schema")
			})

			t.Run("ExecContext", func(t *testing.T) {
				_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE "%s"."test_table" ("C1" INT4, "C2" VARCHAR)`, schema))
				require.NoError(t, err, "it should be able to create a table")
			})

			t.Run("prepared statement", func(t *testing.T) {
				t.Run("QueryRow", func(t *testing.T) {
					stmt, err := db.Prepare(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema))
					require.NoError(t, err, "it should be able to prepare a statement")
					defer func() {
						require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
					}()

					var count int
					err = stmt.QueryRow().Scan(&count)
					require.NoError(t, err, "it should be able to execute a prepared statement")
				})

				t.Run("Exec", func(t *testing.T) {
					stmt, err := db.Prepare(fmt.Sprintf(`INSERT INTO "%s"."test_table" (C1) VALUES (?)`, schema))
					require.NoError(t, err, "it should be able to prepare a statement")
					defer func() {
						require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
					}()
					result, err := stmt.Exec(1)
					require.NoError(t, err, "it should be able to execute a prepared statement")

					_, err = result.LastInsertId()
					require.Error(t, err)
					require.ErrorIs(t, err, driver.ErrNotSupported)

					rowsAffected, err := result.RowsAffected()
					require.NoError(t, err, "it should be able to get rows affected")
					require.EqualValues(t, 1, rowsAffected, "rows affected should be 1")
				})

				t.Run("Query", func(t *testing.T) {
					stmt, err := db.Prepare(fmt.Sprintf(`SELECT C1 FROM "%s"."test_table" WHERE C1 = $1`, schema))
					require.NoError(t, err, "it should be able to prepare a statement")
					defer func() {
						require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
					}()
					rows, err := stmt.Query(1)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					defer func() {
						require.NoError(t, rows.Close(), "it should be able to close the rows")
					}()
					require.True(t, rows.Next(), "it should be able to get a row")
					var c1 int
					err = rows.Scan(&c1)
					require.NoError(t, err, "it should be able to scan the row")
					require.EqualValues(t, 1, c1, "it should be able to get the correct value")
					require.False(t, rows.Next(), "it shouldn't have next row")

					require.NoError(t, rows.Err())
				})
				t.Run("Query with named parameters", func(t *testing.T) {
					stmt, err := db.PrepareContext(ctx, fmt.Sprintf(`SELECT C1, C2 FROM "%s"."test_table" WHERE C1 = :c1_value`, schema))
					require.NoError(t, err, "it should be able to prepare a statement")
					defer func() {
						require.NoError(t, stmt.Close(), "it should be able to close the prepared statement")
					}()
					rows, err := stmt.QueryContext(ctx, sql.Named("c1_value", 1))
					require.NoError(t, err, "it should be able to execute a prepared statement")
					defer func() {
						require.NoError(t, rows.Close(), "it should be able to close the rows")
					}()

					cols, err := rows.Columns()
					require.NoError(t, err, "it should be able to get the columns")
					require.EqualValues(t, []string{"c1", "c2"}, cols, "it should be able to get the correct columns")

					colTypes, err := rows.ColumnTypes()
					require.NoError(t, err, "it should be able to get the column types")
					require.Len(t, colTypes, 2, "it should be able to get the correct number of column types")
					require.EqualValues(t, "INT4", colTypes[0].DatabaseTypeName(), "it should be able to get the correct column type")
					require.EqualValues(t, "VARCHAR", colTypes[1].DatabaseTypeName(), "it should be able to get the correct column type")

					require.True(t, rows.Next(), "it should be able to get a row")
					var c1 int
					var c2 any
					err = rows.Scan(&c1, &c2)
					require.NoError(t, err, "it should be able to scan the row")
					require.EqualValues(t, 1, c1, "it should be able to get the correct value")
					require.Nil(t, c2, "it should be able to get the correct value")
					require.False(t, rows.Next(), "it shouldn't have next row")

					require.NoError(t, rows.Err())
				})
			})

			t.Run("query", func(t *testing.T) {
				t.Run("QueryRow", func(t *testing.T) {
					var count int
					err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema)).Scan(&count)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					require.Equal(t, 1, count, "it should be able to get the correct value")
				})

				t.Run("Exec", func(t *testing.T) {
					result, err := db.Exec(fmt.Sprintf(`INSERT INTO "%s"."test_table" (C1) VALUES ($1)`, schema), 2)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					rowsAffected, err := result.RowsAffected()
					require.NoError(t, err, "it should be able to get rows affected")
					require.EqualValues(t, 1, rowsAffected, "rows affected should be 1")
				})

				t.Run("Query", func(t *testing.T) {
					rows, err := db.Query(fmt.Sprintf(`SELECT C1 FROM "%s"."test_table" WHERE C1 = ?`, schema), 2)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					defer func() {
						require.NoError(t, rows.Close(), "it should be able to close the rows")
					}()
					require.True(t, rows.Next(), "it should be able to get a row")
					var c1 int
					err = rows.Scan(&c1)
					require.NoError(t, err, "it should be able to scan the row")
					require.EqualValues(t, 2, c1, "it should be able to get the correct value")
					require.False(t, rows.Next(), "it shouldn't have next row")

					require.NoError(t, rows.Err())
				})

				t.Run("Query with named parameters", func(t *testing.T) {
					rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT C1 FROM "%s"."test_table" WHERE C1 = :c1_value`, schema), sql.Named("c1_value", 2))
					require.NoError(t, err, "it should be able to execute a prepared statement")
					defer func() {
						require.NoError(t, rows.Close(), "it should be able to close the rows")
					}()

					cols, err := rows.Columns()
					require.NoError(t, err, "it should be able to get the columns")
					require.EqualValues(t, []string{"c1"}, cols, "it should be able to get the correct columns")

					colTypes, err := rows.ColumnTypes()
					require.NoError(t, err, "it should be able to get the column types")
					require.Len(t, colTypes, 1, "it should be able to get the correct number of column types")
					require.EqualValues(t, "INT4", colTypes[0].DatabaseTypeName(), "it should be able to get the correct column type")

					require.True(t, rows.Next(), "it should be able to get a row")
					var c1 int
					err = rows.Scan(&c1)
					require.NoError(t, err, "it should be able to scan the row")
					require.EqualValues(t, 2, c1, "it should be able to get the correct value")
					require.False(t, rows.Next(), "it shouldn't have next row")

					require.NoError(t, rows.Err())
				})
			})

			t.Run("transaction support", func(t *testing.T) {
				t.Run("Begin and Commit", func(t *testing.T) {
					var countBefore int
					err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema)).Scan(&countBefore)
					require.NoError(t, err, "it should be able to execute a prepared statement")

					tx, err := db.Begin()
					require.NoError(t, err, "it should be able to begin a transaction")
					_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."test_table" (C1) VALUES (3)`, schema))
					require.NoError(t, err, "it should be able to execute a prepared statement")
					res, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."test_table" (C1) VALUES (4)`, schema))
					require.NoError(t, err, "it should be able to execute a prepared statement")
					_, err = res.RowsAffected()
					require.Error(t, err, "it should not be able to get rows affected before commit")
					require.ErrorIs(t, err, driver.ErrBeforeCommit)
					_, err = res.LastInsertId()
					require.Error(t, err, "it should not be able to get last insert id before commit")
					require.ErrorIs(t, err, driver.ErrBeforeCommit)

					_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."test_table" (C1) VALUES (?)`, schema), 5)
					require.Error(t, err, "it should not be able to execute a prepared statement with parameters in a transaction")
					require.ErrorIs(t, err, driver.ErrNotSupported)

					var countDuring int
					err = db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema)).Scan(&countDuring)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					require.Equal(t, countBefore, countDuring, "it should not be able to see the changes from the transaction")

					err = tx.Commit()
					require.NoError(t, err, "it should be able to commit the transaction")

					rowsAffected, err := res.RowsAffected()
					require.NoError(t, err, "it should be able to get rows affected after commit")
					require.EqualValues(t, 1, rowsAffected, "rows affected should be 1")

					var countAfter int
					err = db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema)).Scan(&countAfter)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					require.Equal(t, countBefore+2, countAfter, "it should be able to see the changes from the transaction")
				})
				t.Run("BeginTx and Rollback", func(t *testing.T) {
					var countBefore int
					err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema)).Scan(&countBefore)
					require.NoError(t, err, "it should be able to execute a prepared statement")

					tx, err := db.BeginTx(ctx, nil)
					require.NoError(t, err, "it should be able to begin a transaction")
					_, err = tx.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."test_table" (C1) VALUES (5)`, schema))
					require.NoError(t, err, "it should be able to execute a prepared statement")

					var countDuring int
					err = db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema)).Scan(&countDuring)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					require.Equal(t, countBefore, countDuring, "it should not be able to see the changes from the transaction")

					err = tx.Rollback()
					require.NoError(t, err, "it should be able to rollback the transaction")

					var countAfter int
					err = db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."test_table"`, schema)).Scan(&countAfter)
					require.NoError(t, err, "it should be able to execute a prepared statement")
					require.Equal(t, countBefore, countAfter, "changes from the transaction should be rolled back")
				})
			})
		})
	}
}

func GenerateTestSchema() string {
	return strings.ToLower(fmt.Sprintf("tsqlcon_%s_%d", rand.String(12), time.Now().Unix()))
}

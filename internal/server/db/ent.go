package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql/schema"
	"go.uber.org/fx"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	entsql "entgo.io/ent/dialect/sql"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/migrate"
	"github.com/looplj/axonhub/internal/ent/migrate/datamigrate"
	"github.com/looplj/axonhub/internal/ent/migrate/schemahook"
	_ "github.com/looplj/axonhub/internal/ent/runtime"
	_ "github.com/looplj/axonhub/internal/pkg/sqlite"
)

// isPostgresDialect returns true if the dialect name refers to PostgreSQL.
func isPostgresDialect(dialectName string) bool {
	switch dialectName {
	case "postgres", "pgx", "postgresdb", "pg", "postgresql":
		return true
	default:
		return false
	}
}

// openPooledDB opens a database connection, returning the Ent dialect string,
// *sql.DB, and optionally a *pgxpool.Pool (nil for non-PostgreSQL dialects).
// The caller must close the pool during shutdown.
func openPooledDB(ctx context.Context, dialectName, dsn string, maxOpen, maxIdle int, maxLifetime, maxIdleTime, healthCheckPeriod time.Duration) (string, *sql.DB, *pgxpool.Pool, error) {
	if isPostgresDialect(dialectName) {
		return openPostgresDB(ctx, dsn, maxOpen, maxIdle, maxLifetime, maxIdleTime, healthCheckPeriod)
	}
	ed, db, err := openDB(dialectName, dsn, maxOpen, maxIdle, maxLifetime, maxIdleTime)
	return ed, db, nil, err
}

// openPostgresDB opens a PostgreSQL connection using pgxpool.
// pgxpool provides automatic background health checks that periodically ping
// idle connections and replace broken ones, making the connection pool resilient
// to transient network failures and server restarts.
func openPostgresDB(ctx context.Context, dsn string, maxOpen, maxIdle int, maxLifetime, maxIdleTime, healthCheckPeriod time.Duration) (string, *sql.DB, *pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to parse pgxpool config: %w", err)
	}

	if maxOpen > 0 {
		poolConfig.MaxConns = int32(maxOpen)
	}
	if maxIdle > 0 {
		poolConfig.MinConns = int32(maxIdle)
	}
	if maxLifetime > 0 {
		poolConfig.MaxConnLifetime = maxLifetime
	}
	if maxIdleTime > 0 {
		poolConfig.MaxConnIdleTime = maxIdleTime
	}
	if healthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = healthCheckPeriod
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to create pgxpool: %w", err)
	}

	// OpenDBFromPool creates a *sql.DB backed by the pgxpool.
	// The driver manages all connections; the *sql.DB's idle conn settings
	// are unused because the pool itself controls connection lifecycle.
	sqlDB := stdlib.OpenDBFromPool(pool)

	return dialect.Postgres, sqlDB, pool, nil
}

// NewEntClient creates an Ent client. When read_replica.read_dsn is configured,
// SELECT/WITH queries are automatically routed to the replica; all writes go to master.
// Transactions always run on master. If read_dsn is empty, all queries go to master.
//
// When using PostgreSQL dialect, NewEntClient creates a pgxpool.Pool with automatic
// background health checks and registers an fx lifecycle hook to close it on shutdown.
func NewEntClient(cfg Config, lc fx.Lifecycle) *ent.Client {
	var opts []ent.Option
	if cfg.Debug {
		opts = append(opts, ent.Debug())
	}

	ctx := context.Background()

	dbDialect, masterDB, masterPool, err := openPooledDB(ctx, cfg.Dialect, cfg.DSN,
		cfg.MaxOpenConns, cfg.MaxIdleConns, cfg.ConnMaxLifetime, cfg.ConnMaxIdleTime,
		cfg.HealthCheckPeriod)
	if err != nil {
		panic(err)
	}

	// Collect pgxpool instances for lifecycle cleanup.
	var pools []*pgxpool.Pool
	if masterPool != nil {
		pools = append(pools, masterPool)
	}

	var drv dialect.Driver
	if cfg.ReadReplica.DSN != "" {
		readDialect, replicaDB, replicaPool, err := openPooledDB(ctx, cfg.Dialect, cfg.ReadReplica.DSN,
			cfg.ReadReplica.MaxOpenConns, cfg.ReadReplica.MaxIdleConns,
			cfg.ConnMaxLifetime, cfg.ConnMaxIdleTime,
			cfg.HealthCheckPeriod)
		if err != nil {
			panic(err)
		}
		if readDialect != dbDialect {
			panic(fmt.Errorf("read replica dialect mismatch: got %s, want %s", readDialect, dbDialect))
		}
		if replicaPool != nil {
			pools = append(pools, replicaPool)
		}
		masterDriver := entsql.OpenDB(dbDialect, masterDB)
		replicaDriver := entsql.OpenDB(dbDialect, replicaDB)
		drv = newRouterDriver(masterDriver, replicaDriver)
	} else {
		drv = entsql.OpenDB(dbDialect, masterDB)
	}

	// Register lifecycle hook to close pgxpool instances on shutdown.
	// This ensures the background health check goroutines are stopped and
	// connections are properly returned to PostgreSQL via the protocol.
	if lc != nil && len(pools) > 0 {
		pools := pools // capture loop variable
		lc.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				for _, p := range pools {
					p.Close()
				}
				return nil
			},
		})
	}

	opts = append(opts, ent.Driver(drv))
	client := ent.NewClient(opts...)

	err = client.Schema.Create(
		context.Background(),
		migrate.WithGlobalUniqueID(false),
		migrate.WithForeignKeys(false),
		migrate.WithDropIndex(true),
		migrate.WithDropColumn(true),
		schema.WithHooks(schemahook.V0_3_0),
	)
	if err != nil {
		panic(err)
	}

	migrator := datamigrate.NewMigrator(client)
	if err := migrator.Run(context.Background()); err != nil {
		panic(err)
	}

	return client
}

// openDB opens a sql.DB for the given dialect and DSN, applies pool settings,
// and returns the ent dialect string along with the DB handle.
func openDB(dialectName, dsn string, maxOpen, maxIdle int, maxLifetime, maxIdleTime time.Duration) (string, *sql.DB, error) {
	ed, err := entDialect(dialectName)
	if err != nil {
		return "", nil, err
	}

	drvName, err := driverName(dialectName)
	if err != nil {
		return "", nil, err
	}

	sqlDB, err := sql.Open(drvName, dsn)
	if err != nil {
		return "", nil, err
	}

	if maxOpen > 0 {
		sqlDB.SetMaxOpenConns(maxOpen)
	}
	if maxIdle > 0 {
		sqlDB.SetMaxIdleConns(maxIdle)
	}
	if maxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(maxLifetime)
	}
	if maxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(maxIdleTime)
	}

	return ed, sqlDB, nil
}

func driverName(dialectName string) (string, error) {
	switch dialectName {
	case "postgres", "pgx", "postgresdb", "pg", "postgresql":
		return "pgx", nil
	case "sqlite3", "sqlite":
		return "sqlite3", nil
	case "mysql", "tidb":
		return "mysql", nil
	default:
		return "", fmt.Errorf("invalid dialect: %s", dialectName)
	}
}

func entDialect(dialectName string) (string, error) {
	switch dialectName {
	case "postgres", "pgx", "postgresdb", "pg", "postgresql":
		return dialect.Postgres, nil
	case "sqlite3", "sqlite":
		return dialect.SQLite, nil
	case "mysql", "tidb":
		return dialect.MySQL, nil
	default:
		return "", fmt.Errorf("invalid dialect: %s", dialectName)
	}
}

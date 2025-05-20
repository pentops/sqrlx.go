package pgenv

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
	"github.com/pentops/log.go/log"
	"github.com/pentops/sqrlx.go/sqrlx"
)

type DatabaseConfig struct {
	URL          string `env:"POSTGRES_URL"`
	MaxOpenConns int    `env:"POSTGRES_MAX_OPEN_CONNS" default:"10"`
	PingTimeout  int    `env:"POSTGRES_PING_TIMEOUT_SECONDS" default:"10"`
}

func (cfg *DatabaseConfig) OpenPostgres(ctx context.Context) (*sql.DB, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(cfg.PingTimeout))
	defer cancel()

	db, err := sql.Open("postgres", cfg.URL)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)

	for {
		if ctx.Err() != nil {
			return nil, context.Canceled
		}
		if err := db.PingContext(ctx); err != nil {
			log.WithError(ctx, err).Error("pinging PG")
			time.Sleep(time.Second)
			continue
		}
		break
	}

	log.Info(ctx, "connected to PG")

	return db, nil
}

func (cfg *DatabaseConfig) OpenPostgresTransactor(ctx context.Context) (sqrlx.Transactor, error) {
	db, err := cfg.OpenPostgres(ctx)
	if err != nil {
		return nil, err
	}

	return sqrlx.NewPostgres(db), nil
}

package dbtx

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type DBI interface {
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

func NewDatabaseConnection(connectionURI string) DBI {
	conn, err := sql.Open("pgx", connectionURI)
	if err != nil {
		panic("failed connecting to db" + err.Error())
	}

	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed creating logger: " + err.Error())
	}

	logger.Named("db-test") // todo probably need to create a wrapper
	return &DB{
		conn:   conn,
		logger: logger,
	}
}

type DB struct {
	conn   *sql.DB
	logger *zap.Logger
}

func (db *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	startLog, endLog, errLog := db.logs(ctx, "Exec", query, args...)
	defer endLog()
	startLog()

	res, err := db.conn.ExecContext(ctx, query, args...)
	if err != nil {
		errLog(err)
	}
	return res, err
}

func (db *DB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	startLog, endLog, errLog := db.logs(ctx, "Query", query, args...)
	defer endLog()
	startLog()

	res, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		errLog(err)
	}
	return res, err
}

func (db *DB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	startLog, endLog, _ := db.logs(ctx, "Query", query, args...)
	defer endLog()
	startLog()

	res := db.conn.QueryRowContext(ctx, query, args...)
	return res
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	panic("not implemented")
}

func (db *DB) logs(ctx context.Context, method, query string, args ...any) (startLog func(), endLog func(), errLog func(error)) {
	baseLogValues := []zapcore.Field{
		zap.String("method", method),
		zap.String("context-id", fmt.Sprint(ctx.Value("uuid"))),
	}

	extraValues := []zap.Field{
		zap.String("query", query),
		zap.Any("args", args),
	}
	startLog = func() {
		db.logger.Sugar().Info("start", baseLogValues, extraValues)
	}
	endLog = func() {
		db.logger.Sugar().Info("end", baseLogValues)
	}
	errLog = func(err error) {
		db.logger.Sugar().Errorln("err", baseLogValues, extraValues, zap.Error(err))
	}
	return
}

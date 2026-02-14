package dbtx

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
)

type ConnectionI interface {
	InsertOne(ctx context.Context, target Insertable) (int, error)
	InsertMany(ctx context.Context, target MultiInsertable) (int, error) // todo what about ids D':
	SelectOne(ctx context.Context, target Selectable, cdn map[string]any) error
	SelectMany(ctx context.Context, target Selectable, cdn map[string]any) error
}

type Insertable interface {
	Table() string
}

type MultiInsertable interface {
	Table() string
	GetFirstItem() any
	GetItems() []any
	Len() int
}

type Selectable interface {
	Table() string
}

func NewDatabaseConnection(connectionURI string) ConnectionI {
	conn, err := sql.Open("pgx", connectionURI)
	if err != nil {
		panic("failed connecting to db" + err.Error())
	}

	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed creating logger: " + err.Error())
	}

	logger.Named("db-test") // todo probably need to create a wrapper
	return Something{
		conn:   conn,
		logger: logger,
	}
}

type Something struct {
	conn   *sql.DB
	logger *zap.Logger
}

func (db Something) InsertOne(ctx context.Context, target Insertable) (int, error) {
	m, _, err := structToDBMap(target)
	if err != nil {
		return 0, err
	}

	query, args := buildInsert(target.Table(), m)

	//fmt.Printf("query: %s, args: %s\n", query, args)

	_, err = db.conn.ExecContext(ctx, query, args...)
	if err != nil {
		fmt.Println("ExecContext err", err.Error())
		return 0, err
	}

	return 0, nil
}

func (db Something) InsertMany(ctx context.Context, target MultiInsertable) (int, error) {
	db.logger.Debug("InsertMany start", zap.Any("target", target))
	values, columns, err := structDataToDBMap(target)
	if err != nil {
		return 0, err
	}
	query, args := buildInsertMany(target.Table(), columns, values)
	db.logger.Debug("InsertMany will execute", zap.String("query", query), zap.Any("values", values))

	_, err = db.conn.ExecContext(ctx, query, args...)
	if err != nil {
		db.logger.Debug("InsertMany failed execute", zap.Any("err", err))
		return 0, fmt.Errorf("insertMany - failed executing query for %v\nquery: %s\narguments: %s\nerr: %w", target, query, args, err)
	}

	//id, err := result.LastInsertId()
	//if err != nil {
	//	return 0, fmt.Errorf("insertMany - failed getting last insert id for %v\nquery: %s\narguments: %s\nerr: %w", target, query, args, err)
	//}
	db.logger.Debug("InsertMany end", zap.Any("target", target))
	return 0, nil
}

func (db Something) SelectOne(ctx context.Context, target Selectable, cdn map[string]any) error {
	m, _, err := structToDBMap(target)
	if err != nil {
		return err
	}

	query := buildSelect(target.Table(), m, cdn)

	result := db.conn.QueryRowContext(ctx, query)

	err = ScanRow(result, target)
	if err != nil {
		return err
	}

	return nil
}

func (db Something) SelectMany(ctx context.Context, target Selectable, cdn map[string]any) error {
	m, _, err := structToDBMap(target)
	if err != nil {
		return err
	}

	query := buildSelect(target.Table(), m, cdn)

	result, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}

	err = result.Scan(target)
	if err != nil {
		return err
	}

	return nil
}

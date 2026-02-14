package dbtx

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func structToDBMap(v any) (map[string]any, []string, error) {
	val := reflect.ValueOf(v)
	typ := reflect.TypeOf(v)

	if val.Kind() == reflect.Pointer {
		val = val.Elem()
		typ = typ.Elem()
	}

	if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
		elemType := typ.Elem()

		// Create a zero value of the element type
		elemValue := reflect.Zero(elemType)

		return structToDBMap(elemValue.Interface())
	}

	if val.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected struct")
	}

	result := make(map[string]any)
	var arguments []string

	for i := 0; i < typ.NumField(); i++ {
		fieldType := typ.Field(i)
		fieldValue := val.Field(i)

		tag := fieldType.Tag.Get("db")
		if tag == "" || tag == "-" {
			continue
		}

		result[tag] = fieldValue.Interface()
	}

	//for i := 0; i < typ.NumField(); i++ {
	//	fieldType := typ.Field(i)
	//
	//	tag := fieldType.Tag.Get("select")
	//	if tag == "" || tag == "-" {
	//		continue
	//	}
	//
	//	arguments = append(arguments, tag)
	//}

	return result, arguments, nil
}

func structDataToDBMap(v MultiInsertable) ([][]any, []string, error) {
	if v.Len() == 0 {
		return nil, nil, fmt.Errorf("empty slice")
	}

	tags, _, err := structToDBMap(v.GetFirstItem())
	if err != nil {
		return nil, []string{}, err
	}

	result := make([][]any, 0, v.Len())
	columns := make([]string, 0, len(tags))

	for key, _ := range tags {
		columns = append(columns, key)
	}

	for _, item := range v.GetItems() {
		data, _, err := structToDBMap(item) // todo check err, if you're not changing the func
		if err != nil {
			return nil, []string{}, err // todo should return better error
		}
		values := make([]any, 0, len(data))
		for _, col := range columns {
			values = append(values, data[col]) // todo is it correct =?
		}
		result = append(result, values)
	}

	return result, columns, nil
}

func buildInsert(table string, data map[string]any) (string, []any) {
	cols := make([]string, 0, len(data))
	vals := make([]any, 0, len(data))
	placeholders := make([]string, 0, len(data))

	i := 1
	for col, val := range data {
		cols = append(cols, col)
		vals = append(vals, val)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
		i++
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(cols, ", "), // id, feed_id, create_date
		strings.Join(placeholders, ", "),
	)

	return query, vals // [["a", "1234"], ["b", "5678]]
}

func buildInsertMany(table string, columns []string, values [][]any) (string, []any) {
	placeholders := make([]string, len(values))
	vals := make([]any, 0, len(values)*len(columns))

	i := 1
	for r, val := range values {
		ph := make([]string, len(val))
		for index, v := range val {
			vals = append(vals, v)
			ph[index] = fmt.Sprintf("$%d", i)
			i++
		}
		placeholders[r] = fmt.Sprintf("(%s)", strings.Join(ph, ", "))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "), // id, feed_id, create_date
		strings.Join(placeholders, ", "),
	)

	return query, vals
}

func buildSelect(table string, data map[string]any, conditions map[string]any) string {
	cols := make([]string, 0, len(data))

	for col, _ := range data {
		cols = append(cols, col)
	}

	whereQuery := ""
	if len(conditions) > 0 {
		whereQuery = "WHERE"
	}

	for column, value := range conditions {
		// todo in the future might need to add if else for "in ()"
		whereQuery = fmt.Sprintf("%s %s = %v", whereQuery, column, value)
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s %s ORDER BY id DESC",
		strings.Join(cols, ", "),
		table,
		whereQuery,
	)

	return query
}

// ScanRow maps a sql.Row or sql.Rows into a struct pointer using reflection.
// It assumes the order of columns in the SQL query matches the order of fields in the struct.
func ScanRow(scanner interface{ Scan(dest ...any) error }, target any) error {
	v := reflect.ValueOf(target)

	// 1. Ensure target is a pointer
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return errors.New("target must be a non-nil pointer to a struct")
	}

	// 2. Get the element the pointer points to (the struct)
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return errors.New("target must be a pointer to a struct")
	}

	// 3. Collect pointers to every field in the struct
	var fields []any
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)

		// Ensure the field can be set (is exported)
		if field.CanAddr() {
			fields = append(fields, field.Addr().Interface())
		}
	}

	// 4. Scan into the collected field pointers
	return scanner.Scan(fields...)
}

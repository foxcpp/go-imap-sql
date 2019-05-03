package imapsql

import (
	"database/sql"
	"strconv"
	"strings"
)

// db struct is a thin wrapper to solve the most annoying problems
// with cross-RDBMS compatibility.
type db struct {
	DB     *sql.DB
	driver string
	dsn    string
}

func (d db) Prepare(req string) (*sql.Stmt, error) {
	return d.DB.Prepare(d.rewriteSQL(req))
}

func (d db) Query(req string, args ...interface{}) (*sql.Rows, error) {
	return d.DB.Query(d.rewriteSQL(req), args...)
}

func (d db) QueryRow(req string, args ...interface{}) *sql.Row {
	return d.DB.QueryRow(d.rewriteSQL(req), args...)
}

func (d db) Exec(req string, args ...interface{}) (sql.Result, error) {
	return d.DB.Exec(d.rewriteSQL(req), args...)
}

func (d db) Begin() (*sql.Tx, error) {
	return d.DB.Begin()
}

func (d db) Close() error {
	return d.DB.Close()
}

func (d db) rewriteSQL(req string) (res string) {
	res = strings.TrimSpace(req)
	res = strings.TrimLeft(res, "\n\t")
	if d.driver == "postgres" {
		res = ""
		placeholderIndx := 1
		for _, chr := range req {
			if chr == '?' {
				res += "$" + strconv.Itoa(placeholderIndx)
				placeholderIndx += 1
			} else {
				res += string(chr)
			}
		}
		res = strings.TrimLeft(res, "\n\t")
		if strings.HasPrefix(res, "CREATE TABLE") {
			res = strings.Replace(res, "BLOB", "BYTEA", -1)
			res = strings.Replace(res, "LONGTEXT", "BYTEA", -1)
			res = strings.Replace(res, "AUTOINCREMENT", "", -1)
		}
	} else if d.driver == "mysql" {
		if strings.HasPrefix(res, "CREATE TABLE") {
			res = strings.Replace(res, "BIGSERIAL", "BIGINT", -1)
			res = strings.Replace(res, "AUTOINCREMENT", "AUTO_INCREMENT", -1)
		}
		if strings.HasSuffix(res, "ON CONFLICT DO NOTHING") && strings.HasPrefix(res, "INSERT") {
			res = strings.Replace(res, "ON CONFLICT DO NOTHING", "", -1)
			res = strings.Replace(res, "INSERT", "INSERT IGNORE", 1)
		}
	} else if d.driver == "sqlite3" {
		if strings.HasPrefix(res, "CREATE TABLE") {
			res = strings.Replace(res, "BIGSERIAL", "INTEGER", -1)
		}
		if strings.HasSuffix(res, "ON CONFLICT DO NOTHING") && strings.HasPrefix(res, "INSERT") {
			res = strings.Replace(res, "ON CONFLICT DO NOTHING", "", -1)
			res = strings.Replace(res, "INSERT", "INSERT OR IGNORE", 1)
		}
	}

	//log.Println(res)

	return
}

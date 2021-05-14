//Package orm mysql 查询语句封装包
package apijson

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// Client mysql客户端连接
type Client struct {
	NameSrv string
	Proxy   *sql.DB //可以换成任何支持 SQL 协议的引擎，如： postgres 、 mysql
	Tx      *sql.Tx
}

type Next func(rows *sql.Rows) (err error)

//NewOrmClient 创建 Client 指针
var NewOrmClient = func(dataSourceName string) (*Client, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	ormClient := &Client{
		NameSrv: dataSourceName,
		Proxy:   db,
		Tx:      nil,
	}

	return ormClient, nil
}

func nextRows(rows *sql.Rows) (map[string]interface{}, error) {
	var row []interface{}
	tmp := map[string]interface{}{}

	columns, e := rows.Columns()
	if e != nil {
		return nil, e
	}

	columnTypes, e := rows.ColumnTypes()
	if e != nil {
		return nil, e
	}

	columnLen := len(columns)
	for i := 0; i < columnLen; i++ {
		columnType := columnTypes[i]
		dbType := strings.ToLower(columnType.DatabaseTypeName())
		goType, ok := typeForMysqlToGo[dbType]
		if !ok { //默认转化为 string
			goType = "string"
		}

		if nullable, _ := columnType.Nullable(); nullable { //空值
			goType, ok = nullTypeForMysqlToGo[dbType]
			if !ok {
				goType = "NullString"
			}
		}

		switch goType {
		case "bool":
			var val bool
			row = append(row, &val)
		case "int":
			var val int
			row = append(row, &val)
		case "int8":
			var val int8
			row = append(row, &val)
		case "int16":
			var val int16
			row = append(row, &val)
		case "int32":
			var val int32
			row = append(row, &val)
		case "int64":
			var val int64
			row = append(row, &val)
		case "uint":
			var val uint
			row = append(row, &val)
		case "uint8":
			var val uint8
			row = append(row, &val)
		case "uint16":
			var val uint16
			row = append(row, &val)
		case "uint32":
			var val uint32
			row = append(row, &val)
		case "uint64":
			var val uint64
			row = append(row, &val)
		case "float32":
			var val float32
			row = append(row, &val)
		case "float64":
			var val float64
			row = append(row, &val)
		case "string":
			var val string
			row = append(row, &val)
		case "time.Time":
			var val time.Time
			row = append(row, &val)
		case "[]byte":
			var val []byte
			row = append(row, &val)
		case "NullBool":
			var val NullBool
			row = append(row, &val)
		case "NullInt":
			var val NullInt
			row = append(row, &val)
		case "NullFloat":
			var val NullFloat
			row = append(row, &val)
		case "NullString":
			var val NullString
			row = append(row, &val)
		case "NullTime":
			var val NullTime
			row = append(row, &val)
		default:
			var val interface{}
			row = append(row, &val)
		}
	}

	err := rows.Scan(row...)
	if err != nil {
		return nil, err
	}

	for k, column := range columns {
		if row == nil {
			continue
		}

		tmp[column] = row[k]
	}

	return tmp, nil
}

//FindAllMaps 查询符合要求的所有数据，返回 []map[string]string 格式数据
//ctx
//statement 组装的条件
func (c *Client) FindAllMaps(ctx context.Context, statement *Statement) (dest []map[string]interface{}, err error) {
	next := func(rows *sql.Rows) (err error) {
		tmp, err := nextRows(rows)
		if err != nil {
			return err
		}

		dest = append(dest, tmp)
		return nil
	}

	query, err := CreateFindSQL(statement)

	if err != nil {
		return
	}

	err = c.realQuery(ctx, next, query, statement.params...)
	return
}

//FindOneMap 查询符合要求的一条数据，返回格式为 map[string]string
//ctx
//statement 组装的条件
func (c *Client) FindOneMap(ctx context.Context, statement *Statement) (dest map[string]interface{}, err error) {
	next := func(rows *sql.Rows) error {
		dest, err = nextRows(rows)
		if err != nil {
			return err
		}

		return nil
	}

	statement.limit = 1
	query, err := CreateFindSQL(statement)
	if err != nil {
		return
	}

	err = c.realQuery(ctx, next, query, statement.params...)
	return
}

//Count 统计
//ctx
//statement 组装的条件
//count 统计个数
func (c *Client) Count(ctx context.Context, statement *Statement) (uint64, error) {
	query, err := CreateCountSQL(statement)
	var count uint64
	next := func(rows *sql.Rows) error {
		e := rows.Scan(&count)
		if e != nil {
			return e
		}
		return nil
	}
	if err != nil {
		return count, err
	}

	err = c.realQuery(ctx, next, query, statement.params...)
	return count, err
}

//Insert 返回 LastInsertId 和 error
func (c *Client) Insert(ctx context.Context, statement *Statement) (int64, error) {
	query, err := CreateInsertSQL(statement)

	if err != nil {
		return 0, err
	}

	return c.Exec(ctx, query, statement.params...)
}

//InsertIgnore 忽略主键冲突插入，返回 LastInsertId 和 error ，
func (c *Client) InsertIgnore(ctx context.Context, statement *Statement) (int64, error) {
	query, err := CreateInsertIgnoreSQL(statement)
	if err != nil {
		return 0, err
	}
	return c.Exec(ctx, query, statement.params...)
}

//InsertOnDuplicateKeyUpdate insert into on duplicate key update， 表示插入更新数据，当记录中有PrimaryKey，
//或者unique索引的话，如果数据库已经存在数据，则用新数据更新（update），如果没有数据效果则和insert into一样。
//updateKeys 为需要更新的字段
func (c *Client) InsertOnDuplicateKeyUpdate(ctx context.Context,
	statement *Statement, updateKeys map[string]string) (int64, error) {
	query, err := CreateInsertOnDuplicateKeyUpdateSQL(statement, updateKeys)
	if err != nil {
		return 0, err
	}
	return c.Exec(ctx, query, statement.params...)
}

//Replace 替换replace
func (c *Client) Replace(ctx context.Context, statement *Statement, _ ...bool) (int64, error) {
	query, err := CreateReplaceSQL(statement)

	if err != nil {
		return 0, err
	}
	return c.Exec(ctx, query, statement.params...)
}

//Update 返回更新条数
func (c *Client) Update(ctx context.Context, statement *Statement) (int64, error) {
	query, err := CreateUpdateSQL(statement)
	if err != nil {
		return 0, err
	}
	return c.Exec(ctx, query, statement.params...)
}

//Delete DELETE删除，返回删除条数
func (c *Client) Delete(ctx context.Context, statement *Statement) (int64, error) {
	query, err := CreateDeleteSQL(statement)
	if err != nil {
		return 0, err
	}

	return c.Exec(ctx, query, statement.params...)
}

//Exec 原生操作支持，支持自定义sql语句，比如delete，update,insert,replace
//ctx
//query query语句
//args 参数
func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) (int64, error) {
	var ret sql.Result
	var err error

	if c.Tx != nil {
		ret, err = c.Tx.Exec(query, args...)
	} else {
		ret, err = c.Proxy.ExecContext(ctx, query, args...)
	}

	if err != nil {
		return 0, err
	}

	query = strings.TrimSpace(query)
	prefix := strings.ToLower(query[:6])

	if prefix == "update" || prefix == "delete" {
		return ret.RowsAffected()
	}

	return ret.LastInsertId()
}

//Query 原生 Query ，执行mysql select命令
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (ret []map[string]interface{}, err error) {
	ret = []map[string]interface{}{}

	next := func(rows *sql.Rows) (err error) {
		tmp, err := nextRows(rows)
		if err != nil {
			return err
		}

		ret = append(ret, tmp)

		return nil
	}

	err = c.realQuery(ctx, next, query, args...)
	if err != nil {
		return nil, err
	}

	return
}

func (c *Client) realQuery(ctx context.Context, next Next,
	query string, args ...interface{}) (err error) {
	var rows *sql.Rows

	if c.Tx != nil {
		rows, err = c.Tx.Query(query, args...)
		if err != nil {
			return err
		}

		if rows.Err() != nil {
			return rows.Err()
		}

	} else {
		rows, err = c.Proxy.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
	}

	for rows.Next() {
		err = next(rows)
		if err != nil {
			return err
		}
	}

	if rows != nil {
		return rows.Close()
	}

	return
}

//Package orm 数据库 orm statement 语句组成
package apijson

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/iancoleman/orderedmap"
)

//compare 操作符
const (
	OPEqual    = "="  // OPEqual 等于
	OPNotEqual = "<>" // OPNotEqual 不等于
	OPGt       = ">"  // OPGt 大于
	OPGte      = ">=" // OPGte 大于等于
	OPLt       = "<"  // OPLt 小于
	OPLte      = "<=" // OPLte 小于等于
	OPIn       = "{}" // OPIn in
	OPNot      = "!"  // OPNot 去反
	OPLike     = "$"  // OPLike like语句
	OPREG      = "~"  // OPREG 正则表达式
	OPBetween  = "%"  // OPBetween 在某个区间
)

//WhereCond where 语句 map 声明
type WhereCond map[string]interface{}

//Complex 复杂组合语句 map 声明
type Complex map[string]interface{}

//JoinUsings Join Using 结构声明
type JoinUsings []string

//JoinOn Join On 结构声明
type JoinOn map[string]string

//SetMap 更新操作 map 数据声明
type SetMap map[string]interface{}

//Statement 查询语句结构体
type Statement struct {
	cselect   string
	cset      string
	tablename string
	alias     string
	joins     []string
	condition string
	orders    []string
	limit     int32
	offset    int32
	params    []interface{}
	groupby   string
	having    string
	distinct  bool
	forupdate string
}

//NewDbStatement 创建一个数据库语句 Statement
func NewDbStatement() *Statement {
	return &Statement{cselect: "*", distinct: false, limit: -1, offset: -1}
}

//GetParams 获取查询参数
func (statement *Statement) GetParams() []interface{} {
	return statement.params
}

//GetSelect 获取select 字段
func (statement *Statement) GetSelect() string {
	return statement.cselect
}

//GetSet 获取更新字段
func (statement *Statement) GetSet() string {
	return statement.cset
}

//GetTable 获取表名
func (statement *Statement) GetTable() string {
	return statement.tablename
}

//GetAlias 获取别名
func (statement *Statement) GetAlias() string {
	return statement.alias
}

//GetJoins 获取表 Join
func (statement *Statement) GetJoins() []string {
	return statement.joins
}

//GetCondition 获取 condition
func (statement *Statement) GetCondition() string {
	return statement.condition
}

//GetOrder 获取排序 order
func (statement *Statement) GetOrder() string {
	return strings.Join(statement.orders, ",")
}

//GetLimit 获取 limit
func (statement *Statement) GetLimit() int32 {
	return statement.limit
}

//GetOffset 获取 offset
func (statement *Statement) GetOffset() int32 {
	return statement.offset
}

//GetGroupby 获取分组 group by
func (statement *Statement) GetGroupby() string {
	return statement.groupby
}

//GetHaving 获取分组条件 having
func (statement *Statement) GetHaving() string {
	return statement.having
}

//Select 指定要查询的列
func (statement *Statement) Select(cselect ...string) *Statement {
	statement.cselect = strings.Join(cselect, ",")
	return statement
}

//SetTableName 设置表名
//tableName 表名
func (statement *Statement) SetTableName(tableName string) *Statement {
	statement.tablename, statement.alias = alias(tableName)
	return statement
}

//InsertMap 插入数据
func (statement *Statement) InsertMap(attributes SetMap) *Statement {
	return statement.setMap(attributes)
}

//ReplaceMap 替换数据
func (statement *Statement) ReplaceMap(attributes SetMap) *Statement {
	return statement.setMap(attributes)
}

func (statement *Statement) setMap(attributes SetMap) *Statement {
	if len(attributes) == 0 {
		return statement
	}

	var values string
	var fields string

	for key, value := range attributes {
		if fields == "" {
			fields = fmt.Sprint(columnQuote(key))
		} else {
			fields = fmt.Sprint(fields, ",", columnQuote(key))
		}
		if values == "" {
			values = "?"
		} else {
			values = fmt.Sprint(values, ",", "?")
		}
		statement.params = append(statement.params, value)
	}

	statement.cset = fmt.Sprint(" ( ", fields, " ) ", "values", " ( ", values, " ) ")
	return statement
}

//InsertStruct 插入结构体数据，需要 orm 标签注释，如果没有 orm 标签注释，则取字段名，如： Id  int  `orm:"id,int"`
func (statement *Statement) InsertStruct(attributes interface{}) *Statement {
	return statement.setStruct(attributes, true)
}

//ReplaceStruct Replace Struct
func (statement *Statement) ReplaceStruct(attributes interface{}) *Statement {
	return statement.setStruct(attributes, false)
}

func (statement *Statement) setStruct(attributes interface{}, isInsert bool) *Statement {
	v := reflect.ValueOf(attributes)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return statement
	}

	var values string
	var fields string

	ss := structSpecForType(v.Type())

	for i := 0; i < v.NumField(); i++ {
		iv := v.Field(i)
		if iv.CanInterface() {
			name := v.Type().Field(i).Name
			fs := ss.fieldSpec(name)
			if fs == nil {
				continue
			}

			if ((isInsert && (fs.omitinsertempty || fs.omitempty)) || //INSERT 忽略零值
				(!isInsert && (fs.omitreplaceempty || fs.omitempty))) && isEmptyValue(iv) { //REPLACE 忽略零值
				continue
			}

			var interfaceValue interface{}
			//自动插入当前时间，仅在值为零值时才自动赋值
			if ((isInsert && fs.oncreatetime) || fs.onupdatetime) && isEmptyValue(iv) {
				interfaceValue = nowTime(fs.typ)
			} else if fs.oncreatetime && isEmptyValue(iv) {
				continue
			} else {
				interfaceValue = iv.Interface()
			}

			if fields == "" {
				fields = fmt.Sprint(columnQuote(fs.tablecolumn))
			} else {
				fields = fmt.Sprint(fields, ",", columnQuote(fs.tablecolumn))
			}

			if values == "" {
				values = "?"
			} else {
				values = fmt.Sprint(values, ",", "?")
			}

			statement.params = append(statement.params, interfaceValue)
		}
	}

	statement.cset = fmt.Sprint(" ( ", fields, " ) ", "values", " ( ", values, " ) ")
	return statement
}

//getIgnores 获取忽略字段
func getIgnores(arrv reflect.Value, arrLen int) map[string]bool {
	//数组第一个元素
	arrv0 := arrv.Index(0)
	if arrv0.Kind() == reflect.Ptr {
		arrv0 = arrv0.Elem()
	}

	fieldNum := arrv0.NumField()
	arrTyp := arrv0.Type()
	ss := structSpecForType(arrTyp)

	//获取忽略字段
	ignores := map[string]bool{}
	for i := 0; i < fieldNum; i++ {
		v0 := arrv0.Field(i)
		if v0.CanInterface() {
			name := arrTyp.Field(i).Name
			fs := ss.fieldSpec(name)
			if fs == nil {
				continue
			}

			ignores[name] = true
		}
	}

	for k := 0; k < arrLen; k++ {
		kv := arrv.Index(k)
		if kv.Kind() == reflect.Ptr {
			kv = kv.Elem()
		}
		for i := 0; i < fieldNum; i++ {
			iv := kv.Field(i)
			name := arrTyp.Field(i).Name
			if ignore, ok := ignores[name]; ok && ignore {
				if !isEmptyValue(iv) {
					//如果存在非空值，该字段不忽略
					ignores[name] = false
				} else {
					//如果值为空值，但是字段并未忽略空值
					fs := ss.fieldSpec(name)
					if !fs.omitinsertempty && !fs.omitempty {
						ignores[name] = false
					}
				}
			}
		}
	}

	return ignores
}

func (statement *Statement) realInsertStructs(arrv reflect.Value, arrLen int) *Statement {
	//数组第一个元素
	arrv0 := arrv.Index(0)
	if arrv0.Kind() == reflect.Ptr {
		arrv0 = arrv0.Elem()
	}

	fieldNum := arrv0.NumField()
	arrTyp := arrv0.Type()
	ss := structSpecForType(arrTyp)

	//获取忽略字段
	ignores := getIgnores(arrv, arrLen)

	var fields string
	var values string

	//提取字段
	for i := 0; i < fieldNum; i++ {
		name := arrTyp.Field(i).Name
		if ignore, ok := ignores[name]; ok && !ignore {
			fs := ss.fieldSpec(name)
			if fields == "" {
				fields = fmt.Sprint(columnQuote(fs.tablecolumn))
			} else {
				fields = fmt.Sprint(fields, ",", columnQuote(fs.tablecolumn))
			}
		}
	}

	//插入语句
	for k := 0; k < arrLen; k++ {
		kv := arrv.Index(k)
		if kv.Kind() == reflect.Ptr {
			kv = kv.Elem()
		}

		if k == 0 {
			values = "("
		} else {
			values = fmt.Sprint(values, ", (")
		}

		next := true
		for i := 0; i < fieldNum; i++ {
			name := arrTyp.Field(i).Name
			if ignore, ok := ignores[name]; ok && !ignore {
				iv := kv.Field(i)
				fs := ss.fieldSpec(name)

				var interfaceValue interface{}
				if (fs.oncreatetime || fs.onupdatetime) && isEmptyValue(iv) { //自动插入当前时间，仅在值为零值时才自动赋值
					interfaceValue = nowTime(fs.typ)
				} else {
					interfaceValue = iv.Interface()
				}

				if next {
					values = fmt.Sprint(values, "?")
					next = false
				} else {
					values = fmt.Sprint(values, ",", "?")
				}

				statement.params = append(statement.params, interfaceValue)
			}
		}

		values = fmt.Sprint(values, ")")
	}

	statement.cset = fmt.Sprint(" ( ", fields, " ) ", "values", values)
	return statement
}

//InsertStructs 批量插入功能
func (statement *Statement) InsertStructs(attrs interface{}) *Statement {
	arrv := reflect.ValueOf(attrs)

	if arrv.Kind() == reflect.Ptr {
		arrv = arrv.Elem()
	}

	if arrv.Kind() != reflect.Slice && arrv.Kind() != reflect.Array {
		return statement
	}

	arrLen := arrv.Len() //数组长度
	if arrLen <= 0 {
		return statement
	}

	return statement.realInsertStructs(arrv, arrLen)
}

//UpdateMap Update Map
func (statement *Statement) UpdateMap(attributes SetMap) *Statement {
	if len(attributes) == 0 {
		return statement
	}

	var str string
	var params []interface{}
	//update users set name=? where id=?
	for key, value := range attributes {
		if str == "" {
			str = fmt.Sprint(columnQuote(key), "=?")
		} else {
			str = fmt.Sprint(str, ",", columnQuote(key), "=?")
		}
		params = append(params, value)
	}
	statement.params = append(params, statement.params...)
	statement.cset = fmt.Sprint(str)
	return statement
}

//UpdateStruct Update Struct
func (statement *Statement) UpdateStruct(attributes interface{}) *Statement {
	v := reflect.ValueOf(attributes)

	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return statement
	}

	var str string
	var params []interface{}

	ss := structSpecForType(v.Type())

	for i := 0; i < v.NumField(); i++ {
		iv := v.Field(i)
		if iv.CanInterface() {
			name := v.Type().Field(i).Name
			fs := ss.fieldSpec(name)
			if fs == nil {
				continue
			}

			if (fs.omitupdateempty || fs.omitempty) && isEmptyValue(iv) { //UPDATE 忽略零值
				continue
			}

			var interfaceValue interface{}
			if fs.onupdatetime && isEmptyValue(iv) { //修改时自动赋值当前时间，仅在值为零值时才自动赋值
				interfaceValue = nowTime(fs.typ)
			} else if fs.oncreatetime && isEmptyValue(iv) {
				continue
			} else {
				interfaceValue = iv.Interface()
			}

			if str == "" {
				str = fmt.Sprint(columnQuote(fs.tablecolumn), "=?")
			} else {
				str = fmt.Sprint(str, ",", columnQuote(fs.tablecolumn), "=?")
			}

			params = append(params, interfaceValue)
		}
	}

	statement.params = append(params, statement.params...)
	statement.cset = fmt.Sprint(str)
	return statement
}

func nowTime(_type string) interface{} {
	_type = strings.ToLower(_type)
	if _type == "int" || _type == "bigint" || _type == "decimal" {
		return time.Now().Unix()
	}
	return time.Now().Format("2006-01-02 15:04:05")
}

//Where 快捷 where 查询条件组装
func (statement *Statement) Where(where *orderedmap.OrderedMap) *Statement {
	for _, k := range where.Keys() {
		value, ok := where.Get(k)
		if k == "@column" {
			if ok {
				statement.Select(value.(string))
			}
		} else if k == "@order" {
			if ok {
				statement.Order(strings.Replace(strings.Replace(value.(string), "+", " ASC ", -1), "-", " DESC ", -1))
			}
		} else {
			whereImplode(k, value, &statement.condition, &statement.params, "AND")
		}
	}

	return statement
}

//Limit 组装mysql limit 条件，可以使用分页配合使用=
func (statement *Statement) Limit(limit int32) *Statement {
	if limit > 0 {
		statement.limit = limit
	}
	return statement
}

//Offset 组装mysql Offset 条件，可以使用分页配合使用
func (statement *Statement) Offset(offset int32) *Statement {
	if offset >= 0 {
		statement.offset = offset
	}
	return statement
}

//LimitOffset 组装mysql limit offset 条件，可以使用分页配合使用
func (statement *Statement) LimitOffset(limit, offset int32) *Statement {
	statement.Limit(limit)
	statement.Offset(offset)
	return statement
}

//Order 组装mysql Order 条件
//desc 排序方式，false、默认不填为正序，填 true 为逆序
func (statement *Statement) Order(field string, desc ...bool) *Statement {
	var order string

	if len(desc) >= 1 && desc[0] {
		order = "DESC"
	}
	if len(statement.orders) == 0 {
		statement.orders = make([]string, 0)
	}

	statement.orders = append(statement.orders, fmt.Sprint(field, " ", order))

	return statement
}

//GroupBy GROUP BY 分组 group by
func (statement *Statement) GroupBy(group ...string) *Statement {
	if len(group) == 1 {
		statement.groupby = group[0]
	} else if len(group) > 1 {
		statement.groupby = "`" + strings.Join(group, "`,`") + "`"
	}

	return statement
}

//Having having语句
func (statement *Statement) Having(having WhereCond) *Statement {
	replyCondition := ""
	var replyMap []interface{}

	for key, value := range having {
		whereImplode(key, value, &replyCondition, &replyMap, "AND")
	}

	replyCondition = strings.Replace(replyCondition, "( AND", "(", -1)
	replyCondition = strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(replyCondition), "AND"))

	statement.having = replyCondition
	statement.params = append(statement.params, replyMap...)
	return statement
}

//ForUpdate 组装mysql ForUpdate 条件 for update
func (statement *Statement) ForUpdate(forupdate string) *Statement {
	statement.forupdate = forupdate
	return statement
}

//Join  表join语句
func (statement *Statement) Join(table string, relation ...interface{}) *Statement {
	return statement.realjoin(table, "", relation...)
}

//LeftJoin Left JOIN 表 left join 语句
func (statement *Statement) LeftJoin(table string, relation ...interface{}) *Statement {
	return statement.realjoin(table, "LEFT", relation...)
}

//RightJoin RIGHT JOIN 表 right join 语句
func (statement *Statement) RightJoin(table string, relation ...interface{}) *Statement {
	return statement.realjoin(table, "RIGHT", relation...)
}

//InnerJoin INNER JOIN 表inner join 语句
func (statement *Statement) InnerJoin(table string, relation ...interface{}) *Statement {
	return statement.realjoin(table, "INNER", relation...)
}

// FullJoin FULL JOIN 表 full join 语句
func (statement *Statement) FullJoin(table string, relation ...interface{}) *Statement {
	return statement.realjoin(table, "FULL", relation...)
}

//JoinSQL 直接写join 语句
func (statement *Statement) JoinSQL(sql string) *Statement {
	statement.joins = append(statement.joins, sql)
	return statement
}

func (statement *Statement) realjoin(table string, joinDirect string, relation ...interface{}) *Statement {
	table, joinalias := alias(table)

	joinStatement := joinDirect + " JOIN `" + table + "` "
	if joinalias != "" {
		joinStatement = joinStatement + "AS `" + joinalias + "` "
	}

	if len(relation) > 0 {
		rela := relation[0]

		v := reflect.ValueOf(rela)

		if v.Kind() == reflect.String {
			joinStatement = joinStatement + "USING (`" + rela.(string) + "`) "
		} else if isArray(v) {
			relations, ok := rela.(JoinUsings)
			if ok {
				joinStatement = joinStatement + "USING (`" + strings.Join(relations, "`,`") + "`) "
			}
		} else if v.Kind() == reflect.Map {
			joinStatement = joinStatement + "ON "

			for _, k := range v.MapKeys() {
				key := k.String()
				if value, ok := v.MapIndex(k).Interface().(string); ok {
					var tableColumn string
					dotIndex := strings.Index(key, ".")
					if dotIndex != -1 {
						tableColumn = columnQuote(key)
					} else {
						if statement.alias != "" {
							tableColumn = "`" + statement.alias + "`.`" + key + "`"
						} else {
							tableColumn = "`" + statement.tablename + "`.`" + key + "`"
						}
					}

					if joinalias != "" {
						joinStatement = joinStatement + tableColumn + "=`" + joinalias + "`.`" + value + "` AND"
					} else {
						joinStatement = joinStatement + tableColumn + "=`" + table + "`.`" + value + "` AND"
					}
				}
			}

			joinStatement = strings.TrimRight(joinStatement, "AND")
		}
	}

	statement.joins = append(statement.joins, joinStatement)
	return statement
}

//where条件
func whereImplode(key string, value interface{}, replyCondition *string,
	replyMap *[]interface{}, connector string) {
	v := reflect.ValueOf(value)

	column, operator, orAnd, not := pregOperatorMatch(key)

	if column != "" {
		column = columnQuote(column)

		switch operator {
		case "", OPEqual:
			if value == nil {
				*replyCondition = *replyCondition + " " + connector + column + "IS " + not + " NULL "
			} else if isArray(v) {
				*replyCondition = *replyCondition + " " + connector + column + not + " IN ("
				handleWhereIn(v, replyCondition, replyMap)
				*replyCondition = *replyCondition + ") "
			} else {
				*replyMap = append(*replyMap, value)
				if not == "" {
					*replyCondition = *replyCondition + " " + connector + column + "= ? "
				} else {
					*replyCondition = *replyCondition + " " + connector + column + "!= ? "
				}
			}
		case OPGt, OPGte, OPLt, OPLte:
			*replyMap = append(*replyMap, value)
			*replyCondition = *replyCondition + " " + connector + column + operator + " ? "
		case OPIn:
			if isArray(v) {
				*replyCondition = *replyCondition + " " + connector + column + not + " IN ("
				handleWhereIn(v, replyCondition, replyMap)
				*replyCondition = *replyCondition + ") "
			} else {
				strVal, ok := value.(string)
				if ok {
					if strVal == "=null" {
						*replyCondition = *replyCondition + " " + connector + column + "IS NULL "
					} else if strVal == "!=null" {
						*replyCondition = *replyCondition + " " + connector + column + "IS NOT NULL "
					} else {
						*replyCondition = *replyCondition + " " + connector + not + " ("
						handleOrAnd(orAnd, column, strVal, replyCondition, replyMap)
						*replyCondition = *replyCondition + ") "
					}
				}
			}
		case OPLike, OPREG:
			op := " LIKE "
			if operator == OPREG {
				op = " REGEXP "
			}

			if isArray(v) {
				*replyCondition = *replyCondition + " " + connector + not + " ("
				handleLikeArray(v, &orAnd, &column, &op, replyCondition, replyMap)
				*replyCondition = *replyCondition + ") "
			} else {
				*replyMap = append(*replyMap, value)
				*replyCondition = *replyCondition + " " + connector + column + not + op + " ? "
			}
		case OPBetween:
			if isArray(v) {
				*replyCondition = *replyCondition + " " + connector + not + " ("
				handleBetween(v, &orAnd, &column, replyCondition, replyMap)
				*replyCondition = *replyCondition + ") "
			} else {
				betweenVal, ok := value.(string)
				if ok {
					start, end := getBetweenStartEnd(betweenVal)
					*replyMap = append(*replyMap, start)
					*replyMap = append(*replyMap, end)
					*replyCondition = *replyCondition + " " + connector + not + " (" + column + " BETWEEN ? AND ?) "
				}
			}
		}
	}

	*replyCondition = strings.TrimLeft(*replyCondition, " ")
	*replyCondition = strings.TrimLeft(*replyCondition, connector)
}

//列处理
func columnQuote(str string) (tableColumn string) {
	dotIndex := strings.Index(str, ".")
	if dotIndex != -1 {
		tableColumn = " `" + strings.TrimSpace(str[0:dotIndex]) + "`.`" + strings.TrimSpace(str[dotIndex+1:]) + "` "
	} else {
		tableColumn = " `" + strings.TrimSpace(str) + "` "
	}
	return
}

//表别名
func alias(src string) (table, alias string) {
	start := strings.Index(src, "(")

	if start != -1 {
		end := strings.Index(src, ")")
		if end != -1 {
			return src[:start], src[start+1 : end]
		}
	}

	return src, ""
}

//匹配操作符
func pregOperatorMatch(key string) (column, operator, orAnd, not string) {
	if key == "" {
		return
	}

	l := len(key)

	var last1, last2, last3 string

	last1 = key[l-1 : l]

	if l >= 2 {
		last2 = key[l-2 : l]
	}

	if l >= 3 {
		last3 = key[l-3 : l]
	}

	orAnd = "OR"
	index := l

	switch last1 {
	case "}":
		switch last2 {
		case "{}":
			switch last3 {
			case "&{}":
				orAnd = "AND"
				index = l - 3
			case "|{}":
				index = l - 3
			case "!{}":
				not = " NOT "
				index = l - 3
			default:
				index = l - 2
			}
			operator = "{}"
		}
	case ">":
		switch last2 {
		case "<>":
			operator = "<>"
			index = l - 2
		default:
			operator = ">"
			index = l - 1
		}
	case "<":
		operator = "<"
		index = l - 1
	case "=":
		switch last2 {
		case ">=":
			operator = ">="
			index = l - 2
		case "<=":
			operator = "<="
			index = l - 2
		}
	case "@":
		switch last2 {
		case "{@":
			switch last3 {
			case "}{@":
				operator = "}{@"
				index = l - 3
			}
		}
	case ")":
		switch last2 {
		case "()":
			operator = "()"
			index = l - 2
		}
	case "$":
		switch last2 {
		case "&$":
			orAnd = "AND"
			index = l - 2
		case "|$":
			index = l - 2
		case "!$":
			not = " NOT "
			index = l - 2
		default:
			index = l - 1
		}
		operator = "$"
	case "~":
		switch last2 {
		case "!~":
			operator = "!~"
			index = l - 2
		default:
			operator = "~"
			index = l - 1
		}
	case "%":
		switch last2 {
		case "|%":
			index = l - 2
		case "&%":
			orAnd = "AND"
			index = l - 2
		case "!%":
			not = " NOT "
			index = l - 2
		default:
			index = l - 1
		}
		operator = "%"
	case "+":
		operator = "+"
		index = l - 1
	case "-":
		operator = "-"
		index = l - 1
	case "!":
		operator = "="
		index = l - 1
		not = " NOT "
	}

	column = strings.TrimSpace(key[0:index])
	return
}

//判断是否 Array 或 Slice
func isArray(v reflect.Value) bool {
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
}

//处理 IN 或者 NOT IN 语句
func handleWhereIn(inValue reflect.Value, replyCondition *string, replyMap *[]interface{}) {
	l := inValue.Len()

	if l == 0 {
		return
	}

	*replyMap = append(*replyMap, inValue.Index(0).Interface())
	*replyCondition = *replyCondition + "?"

	for i := 1; i < l; i++ {
		*replyMap = append(*replyMap, inValue.Index(i).Interface())
		*replyCondition = *replyCondition + ", ?"
	}

}

//处理 或 语句
func handleOrAnd(orAnd, column, value string,
	replyCondition *string, replyMap *[]interface{}) {
	arrVal := strings.Split(value, ",")
	if len(arrVal) == 0 {
		return
	}

	i := 0
	for _, val := range arrVal {
		realVal, operator := getOrOp(val)

		if i == 0 {
			*replyCondition = *replyCondition + column + operator + " ? "
		} else {
			*replyCondition = *replyCondition + " " + orAnd + " " + column + operator + "? "
		}
		i++

		*replyMap = append(*replyMap, realVal)
	}
}

//获取 BETWEEN 语句起始值
func getBetweenStartEnd(value string) (start, end string) {
	arrVal := strings.Split(value, ",")
	if len(arrVal) != 2 {
		return
	}
	start = arrVal[0]
	end = arrVal[1]
	return
}

//
func getOrOp(val string) (value, operator string) {
	if val == "" {
		return
	}

	var v0, v1 string

	l := len(val)
	v0 = val[0:1]

	if l > 2 {
		v1 = val[0:2]
	}

	index := 0

	switch v0 {
	case ">":
		switch v1 {
		case ">=":
			operator = ">="
			index = 2
		default:
			operator = ">"
			index = 1
		}
	case "<":
		switch v1 {
		case "<=":
			operator = "<="
			index = 2
		default:
			operator = "<"
			index = 1
		}
	default:
		operator = "="
	}

	value = strings.TrimSpace(val[index:])
	return
}

//处理 LIKE 数组
func handleLikeArray(inValue reflect.Value, orAnd, column, op, replyCondition *string, replyMap *[]interface{}) {
	l := inValue.Len()

	for i := 0; i < l; i++ {
		*replyMap = append(*replyMap, inValue.Index(i).Interface())
		*replyCondition = *replyCondition + *column + *op + " ? " + *orAnd
	}

	*replyCondition = strings.TrimRight(*replyCondition, *orAnd)
}

//处理 BETWEEN 数组
func handleBetween(inValue reflect.Value, orAnd, column, replyCondition *string, replyMap *[]interface{}) {
	l := inValue.Len()

	for i := 0; i < l; i++ {
		betweenStr, ok := inValue.Index(i).Interface().(string)
		if ok {
			start, end := getBetweenStartEnd(betweenStr)
			*replyMap = append(*replyMap, start)
			*replyMap = append(*replyMap, end)
			*replyCondition = *replyCondition + *column + " BETWEEN" + " ? AND ? " + *orAnd + " "
		}
	}

	*replyCondition = strings.TrimRight(strings.TrimSpace(*replyCondition), *orAnd)
}

//struct 标签解析结果
type fieldSpec struct {
	name             string //字段名
	i                int    //位置
	index            []int
	tablecolumn      string //对应数据库表列名
	typ              string //数据库表字段类型，TINYINT, INT, DOUBLE, VARCHAR ....
	omitinsertempty  bool   //INSERT 时忽略零值
	omitreplaceempty bool   //REPLACE 时忽略零值
	omitupdateempty  bool   //UPDATE 时忽略零值
	omitempty        bool   //INSERT、REPLACE、UPDATE 时都忽略零值，例如 Auto Increment
	oncreatetime     bool   //INSERT 时初始化为当前时间，具体格式根据 typ 决定，如果是数字类型包括 INT、BIGINT等，则是时间戳，否则就是 "2019-01-25" 这种格式
	onupdatetime     bool   //数据变更时修改为当前时间，具体格式根据 typ 决定，这里我推荐数据库自带的时间戳更新功能。
}

func (ss *structSpec) fieldSpec(name string) *fieldSpec {
	return ss.m[name]
}

//根据表字段获取 fieldSpec
func (ss *structSpec) fieldSpecByColumn(columnName string) *fieldSpec {
	return ss.cm[columnName]
}

type structSpec struct {
	m  map[string]*fieldSpec
	cm map[string]*fieldSpec
	l  []*fieldSpec
}

var (
	structSpecMutex sync.RWMutex
	structSpecCache = make(map[reflect.Type]*structSpec)
)

func structSpecForType(t reflect.Type) *structSpec {
	structSpecMutex.RLock()
	ss, found := structSpecCache[t]
	structSpecMutex.RUnlock()
	if found {
		return ss
	}

	structSpecMutex.Lock()
	defer structSpecMutex.Unlock()
	ss, found = structSpecCache[t]
	if found {
		return ss
	}

	ss = &structSpec{m: make(map[string]*fieldSpec), cm: make(map[string]*fieldSpec)}
	compileStructSpec(t, make(map[string]int), nil, ss)
	structSpecCache[t] = ss
	return ss
}

func compileStructSpec(t reflect.Type, depth map[string]int,
	index []int, ss *structSpec) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		switch {
		case f.PkgPath != "" && !f.Anonymous:
			// Ignore unexported fields.
		case f.Anonymous:
			// TODO: Handle pointers. Requires change to decoder and
			// protection against infinite recursion.
			if f.Type.Kind() == reflect.Struct {
				compileStructSpec(f.Type, depth, append(index, i), ss)
			}
		default:
			fs := &fieldSpec{name: f.Name, i: i}
			tag := f.Tag.Get("orm")
			if tag == "" {
				tag = f.Name
				fs.tablecolumn = f.Name
			}

			p := strings.Split(tag, ",")
			if len(p) > 0 {
				if p[0] == "-" {
					continue
				}

				if len(p[0]) > 0 {
					fs.tablecolumn = p[0]
				}

				if len(p) > 1 && len(p[1]) > 0 {
					fs.typ = p[1]
				}

				if len(p) > 2 {
					for _, s := range p[2:] {
						passFieldSpec(t, s, fs)
					}
				}
			}
			d, found := depth[fs.name]
			if !found {
				d = 1 << 30
			}
			switch {
			case len(index) == d:
				// At same depth, remove from result.
				delete(ss.m, fs.name)
				j := 0
				for k := 0; k < len(ss.l); k++ {
					if fs.name != ss.l[k].name {
						ss.l[j] = ss.l[k]
						j++
					}
				}
				ss.l = ss.l[:j]
			case len(index) < d:
				fs.index = make([]int, len(index)+1)
				copy(fs.index, index)
				fs.index[len(index)] = i
				depth[fs.name] = len(index)
				ss.m[fs.name] = fs
				ss.cm[fs.tablecolumn] = fs
				ss.l = append(ss.l, fs)
			}
		}
	}
}

func passFieldSpec(t reflect.Type, s string, fs *fieldSpec) {
	switch s {
	case "omitinsertempty":
		fs.omitinsertempty = true
	case "omitreplaceempty":
		fs.omitreplaceempty = true
	case "omitupdateempty":
		fs.omitupdateempty = true
	case "omitempty":
		fs.omitempty = true
	case "oncreatetime":
		fs.oncreatetime = true
	case "onupdatetime":
		fs.onupdatetime = true
	default:
		panic(fmt.Errorf("redigo: unknown field tag %s for type %s", s, t.Name()))
	}
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	default:
		typ := v.Type().String()
		if typ == "time.Time" {
			if v.CanInterface() {
				t, ok := v.Interface().(time.Time)
				if ok && !t.IsZero() {
					return false
				}
				return true
			}
		}
	}
	return false
}

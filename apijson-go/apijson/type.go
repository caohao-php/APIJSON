package apijson

import (
	"fmt"
	"strconv"
	"time"
)

//NullString 数据库 varchar NULL 类型
type NullString string

//NullInt 数据库 int NULL 类型
type NullInt int

//NullFloat 数据库 double/float NULL 类型
type NullFloat float64

//NullBool 数据库 bool NULL 类型
type NullBool bool

//NullBool 数据库 bool NULL 类型
type NullTime string

//TimeLayout time.Parse 解析数据库时间字段到 NullTime 时的 layout，默认为 "2006-01-02 15:04:05 +0800 CST"
var TimeLayout string

//Scan NullString 类型实现msyql引擎查询赋值接口
func (ns *NullString) Scan(value interface{}) error {
	if value == nil {
		*ns = ""
		return nil
	}

	tmp, err := rowToString(value)
	if err != nil {
		return err
	}

	*ns = NullString(tmp)

	return nil
}

//Scan NullInt 类型实现msyql引擎查询赋值接口
func (ns *NullInt) Scan(value interface{}) error {
	if value == nil {
		*ns = 0
		return nil
	}

	tmp, err := rowToString(value)

	if err != nil {
		return err
	}

	if tmp == "" {
		*ns = 0
		return nil
	}

	i, err := strconv.Atoi(tmp)

	if err == nil {
		*ns = NullInt(i)
	}

	return err
}

//Scan NullBool 类型实现msyql引擎查询赋值接口
func (ns *NullBool) Scan(value interface{}) error {
	if value == nil {
		*ns = false
		return nil
	}

	tmp, err := rowToString(value)
	if err != nil {
		return err
	}

	if tmp == "true" || tmp == "1" || tmp == "yes" {
		*ns = true
		return nil
	}

	*ns = false
	return nil
}

//Scan NullFloat 类型实现msyql引擎查询赋值接口
func (ns *NullFloat) Scan(value interface{}) error {
	if value == nil {
		*ns = 0
		return nil
	}

	tmp, err := rowToString(value)
	if err != nil {
		return err
	}

	if tmp == "" {
		*ns = 0
		return nil
	}

	i, err := strconv.ParseFloat(tmp, 32)

	if err == nil {
		*ns = NullFloat(i)
	}

	return err
}

//Scan NullTime 类型实现msyql引擎查询赋值接口，
func (ns *NullTime) Scan(value interface{}) error {
	*ns = ""

	if value == nil {
		return nil
	}

	tmp, err := rowToString(value)
	if err != nil {
		return nil
	}

	if tmp == "" {
		return nil
	}

	if TimeLayout == "" {
		TimeLayout = "2006-01-02 15:04:05 +0800 CST"
	}

	i, err := time.Parse(TimeLayout, tmp)
	if err == nil {
		*ns = NullTime(i.Format("2006-01-02 15:04:05"))
	}

	return nil
}

func rowToString(row interface{}) (ret string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("query error: %v", p)
		}
	}()

	if row == nil {
		return "", nil
	}

	switch v := row.(type) {
	case nil:
		return "", nil
	case *string:
		return fmt.Sprintf("%v", *v), nil
	case *bool:
		return fmt.Sprintf("%v", *v), nil
	case *uint8:
		return fmt.Sprintf("%v", *v), nil
	case *uint16:
		return fmt.Sprintf("%v", *v), nil
	case *uint32:
		return fmt.Sprintf("%v", *v), nil
	case *uint64:
		return fmt.Sprintf("%v", *v), nil
	case *int8:
		return fmt.Sprintf("%v", *v), nil
	case *int16:
		return fmt.Sprintf("%v", *v), nil
	case *int32:
		return fmt.Sprintf("%v", *v), nil
	case *int64:
		return fmt.Sprintf("%v", *v), nil
	case *float32:
		return fmt.Sprintf("%v", *v), nil
	case *float64:
		return fmt.Sprintf("%v", *v), nil
	case *int:
		return fmt.Sprintf("%v", *v), nil
	case *uint:
		return fmt.Sprintf("%v", *v), nil
	case *[]byte:
		return fmt.Sprintf("%v", *v), nil
	case string, bool, uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64, int, uint:
		return fmt.Sprintf("%v", v), nil
	case []byte:
		return string(v), nil
	case *interface{}:
		return rowToString(*v)
	case interface{}:
		switch vv := v.(type) {
		case string, bool, uint8, uint16, uint32, uint64, int8, int16, int32, int64, float32, float64, int, uint:
			return fmt.Sprintf("%v", vv), nil
		case []byte:
			return string(vv), nil
		default:
			return fmt.Sprintf("%v", vv), nil
		}
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

//日期时间类型
var typeForMysqlToGo = map[string]string{
	"bool":               "bool",
	"int":                "int",
	"integer":            "int",
	"tinyint":            "int8",
	"smallint":           "int16",
	"mediumint":          "int32",
	"bigint":             "int64",
	"int unsigned":       "uint",
	"integer unsigned":   "uint",
	"tinyint unsigned":   "uint8",
	"smallint unsigned":  "uint16",
	"mediumint unsigned": "uint32",
	"bigint unsigned":    "uint64",
	"bit":                "int8",
	"float":              "float32",
	"double":             "float64",
	"decimal":            "float64",
	"enum":               "string",
	"set":                "string",
	"varchar":            "string",
	"char":               "string",
	"tinytext":           "string",
	"mediumtext":         "string",
	"text":               "string",
	"longtext":           "string",
	"blob":               "string",
	"tinyblob":           "string",
	"mediumblob":         "string",
	"longblob":           "string",
	"date":               "string",
	"time":               "string",
	"datetime":           "NullTime",
	"timestamp":          "NullTime",
	"binary":             "[]byte",
	"varbinary":          "[]byte",
}

var nullTypeForMysqlToGo = map[string]string{
	"bool":               "NullBool",
	"int":                "NullInt",
	"integer":            "NullInt",
	"tinyint":            "NullInt",
	"smallint":           "NullInt",
	"mediumint":          "NullInt",
	"bigint":             "NullInt",
	"int unsigned":       "NullInt",
	"integer unsigned":   "NullInt",
	"tinyint unsigned":   "NullInt",
	"smallint unsigned":  "NullInt",
	"mediumint unsigned": "NullInt",
	"bigint unsigned":    "NullInt",
	"bit":                "NullInt",
	"float":              "NullFloat",
	"double":             "NullFloat",
	"decimal":            "NullFloat",
	"enum":               "NullString",
	"set":                "NullString",
	"varchar":            "NullString",
	"char":               "NullString",
	"tinytext":           "NullString",
	"mediumtext":         "NullString",
	"text":               "NullString",
	"longtext":           "NullString",
	"blob":               "NullString",
	"tinyblob":           "NullString",
	"mediumblob":         "NullString",
	"longblob":           "NullString",
	"date":               "NullString",
	"time":               "NullString",
	"datetime":           "NullTime",
	"timestamp":          "NullTime",
	"binary":             "NullString",
	"varbinary":          "NullString",
}

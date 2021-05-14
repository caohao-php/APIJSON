package apijson

import (
	"fmt"
	"strings"
)

func findSQL(statement *Statement) (sql string) {
	if statement.alias != "" {
		sql = fmt.Sprint("SELECT ", statement.cselect, " FROM `", statement.tablename, "` AS `", statement.alias, "`")
	} else {
		sql = fmt.Sprint("SELECT ", statement.cselect, " FROM `", statement.tablename, "`")
	}

	if len(statement.joins) > 0 {
		for _, j := range statement.joins {
			sql = fmt.Sprint(sql, " ", j, " ")
		}
	}

	if statement.condition != "" {
		sql = fmt.Sprint(sql, " WHERE ", statement.condition)
	}

	if statement.groupby != "" {
		sql = fmt.Sprint(sql, " GROUP BY ", statement.groupby)
	}

	if statement.having != "" {
		sql = fmt.Sprint(sql, " HAVING ", statement.having)
	}

	return
}

//CreateFindSQL 组装mysql 语句
func CreateFindSQL(statement *Statement) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}

	sql = findSQL(statement)

	if len(statement.orders) > 0 {
		sql = fmt.Sprint(sql, " ORDER BY ", statement.GetOrder())
	}

	if statement.limit >= 0 {
		sql = fmt.Sprint(sql, " LIMIT ", statement.limit)
	}

	if statement.offset >= 0 {
		sql = fmt.Sprint(sql, " OFFSET ", statement.offset)
	}

	if statement.forupdate != "" {
		sql = fmt.Sprint(sql, " ", statement.forupdate)
	}

	return
}

const countSQLPrefix = "count("

//CreateCountSQL 创建 count 语句
func CreateCountSQL(statement *Statement) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}

	if statement.cselect == "*" {
		statement.cselect = "count(*)"
	} else {
		str := countSQLPrefix
		if statement.distinct {
			str = "count( DISTINCT"
		}
		if len(statement.cselect) <= 6 {
			statement.cselect = fmt.Sprint(str, statement.cselect, ")")
		} else {
			statement.cselect = strings.TrimSpace(statement.cselect)
			prefix := strings.ToLower(statement.cselect[:6])
			if prefix != countSQLPrefix {
				statement.cselect = fmt.Sprint(str, statement.cselect, ")")
			}
		}
	}

	sql = findSQL(statement)
	return
}

//CreateInsertSQL 创建 insert 语句
func CreateInsertSQL(statement *Statement) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}
	sql = fmt.Sprint("INSERT INTO `", statement.tablename, "` ", statement.cset)
	return sql, nil
}

//CreateReplaceSQL 创建 replace 语句
func CreateReplaceSQL(statement *Statement) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}
	sql = fmt.Sprint("REPLACE INTO `", statement.tablename, "` ", statement.cset)
	return sql, nil
}

//CreateInsertIgnoreSQL 创建 insert ignore 语句
func CreateInsertIgnoreSQL(statement *Statement) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}
	sql = fmt.Sprint("INSERT IGNORE INTO `", statement.tablename, "` ", statement.cset)
	return sql, nil
}

//CreateInsertOnDuplicateKeyUpdateSQL 创建 INSERT INTO **** ON DUPLICATE KEY UPDATE 语句
func CreateInsertOnDuplicateKeyUpdateSQL(statement *Statement, updateKeys map[string]string) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}
	sql = fmt.Sprint("INSERT INTO `", statement.tablename, "` ", statement.cset, " ON DUPLICATE KEY UPDATE ")

	if len(updateKeys) != 0 {
		i := 0
		for k, v := range updateKeys {
			if !strings.HasPrefix(v, "VALUES") && !strings.HasPrefix(v, "values") {
				statement.params = append(statement.params, v)
				v = "?"
			}

			if i == 0 {
				sql = fmt.Sprint(sql, " `", k, "` = ", v)
			} else {
				sql = fmt.Sprint(sql, ", `", k, "` = ", v)
			}
			i++
		}
	}

	return sql, nil
}

//CreateUpdateSQL 创建 update 语句
func CreateUpdateSQL(statement *Statement) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}
	sql = fmt.Sprint("UPDATE `", statement.tablename, "` SET ", statement.cset)
	if statement.condition != "" {
		sql = fmt.Sprint(sql, " WHERE ", statement.condition)
	}
	if len(statement.orders) > 0 {
		sql = fmt.Sprint(sql, " ORDER BY ", statement.GetOrder())
	}
	if statement.limit >= 0 {
		sql = fmt.Sprint(sql, " LIMIT ", statement.limit)
	}
	if statement.offset >= 0 {
		sql = fmt.Sprint(sql, " OFFSET ", statement.offset)
	}
	return sql, nil
}

//CreateDeleteSQL 创建 delete 语句
func CreateDeleteSQL(statement *Statement) (sql string, err error) {
	if statement.tablename == "" {
		return "", fmt.Errorf("orm: table empty")
	}
	sql = fmt.Sprint("DELETE FROM `", statement.tablename, "` ")
	if statement.condition != "" {
		sql = fmt.Sprint(sql, " WHERE ", statement.condition)
	}
	if len(statement.orders) > 0 {
		sql = fmt.Sprint(sql, " ORDER BY ", statement.GetOrder())
	}
	if statement.limit >= 0 {
		sql = fmt.Sprint(sql, " LIMIT ", statement.limit)
	}
	if statement.offset >= 0 {
		sql = fmt.Sprint(sql, " OFFSET ", statement.offset)
	}
	return sql, nil
}

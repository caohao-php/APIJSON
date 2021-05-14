package apijson

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"
)

const (
	//是否数组
	IsArrayFalse = 0 //非数组
	IsArrayTrue  = 1 //数组元素
	IsArrayField = 2 //数组字段提取
)

type Join struct {
	Type  string //join 类型 LEFT JOIN, RIGHT JOIN, INNER JOIN, FULL JOIN, OUTTER JOIN
	Table string //关联副表
	Field string //关联字段
}

type ParseTree struct {
	Key          string                              //key
	IsArray      bool                                //是否数组
	Size         int                                 //数组大小
	SQLCount     int                                 //sql count
	Joins        map[string]*Join                    //表 join
	Children     []*ParseTree                        //子节点（在数组元素下的 '[]' 会有多个子节点）
	Parent       *ParseTree                          //父节点
	First        *ParseTree                          //兄弟节点，老大
	Next         *ParseTree                          //兄弟节点，弟弟
	Prev         *ParseTree                          //兄弟节点，哥哥
	Index        int                                 //子节点索引 index
	IsFieldArray bool                                //是否字段提取数组
	FieldData    [][]interface{}                     //字段数组提取数据
	Data         []map[string]map[string]interface{} //数据
}

func Parse(ctx context.Context, dataSourceName string, reqbody []byte) ([]byte, error) {
	req := orderedmap.New()
	err := json.Unmarshal(reqbody, &req)

	db, err := NewOrmClient(dataSourceName)
	if err != nil {
		return nil, err
	}

	head := ParseTree{}
	err = ParseNode(ctx, req, 0, &head, &head, db)

	ret := orderedmap.New()

	if head.IsArray {
		if head.IsFieldArray {
			ret.Set(head.Key, &head.FieldData[0])
		} else {
			sub := []*orderedmap.OrderedMap{}
			ret.Set(head.Key, &sub)
			encodeArrayResult(head.Children[0], head.Size, &sub)
		}
	} else {
		encodeResult(&head, ret)
	}

	return ret.MarshalJSON()
}

func ParseNode(ctx context.Context, req *orderedmap.OrderedMap,
	index int, head, node *ParseTree, db *Client) error {
	for _, k := range req.Keys() {
		v, ok := getSubMap(req, k)
		if !ok {
			continue
		}

		if node.Key == "" {
			node.Key = k
		} else {
			node = getSliblingNode(k, node)
		}

		isKeyArray := isKeyArray(k)

		if isKeyArray == IsArrayTrue || isKeyArray == IsArrayField {
			//数组或者数组提取
			node.IsArray = true

			if node.Parent != nil && node.Parent.IsArray {
				for i := 0; i < node.Parent.Size; i++ {
					err := parseArray(ctx, isKeyArray, i, k, v, head, node, db)
					if err != nil {
						return err
					}
				}
			} else {
				err := parseArray(ctx, isKeyArray, 0, k, v, head, node, db)
				if err != nil {
					return err
				}
			}

			if isKeyArray == IsArrayField { //数组字段提取
				node.IsFieldArray = true
				node.Children = nil
				node.Data = nil
				node.Size = len(node.FieldData)
			}
		} else {
			if node.Parent != nil && node.Parent.IsArray {
				if node.First == nil { //数组元素的第一个节点
					if len(node.Parent.Joins) > 0 { //解析join
						err := parseJoin(req, head, node)
						if err != nil {
							return err
						}
					}

					ds, err := findAll(ctx, k, v, index, head, node, db)
					if err != nil {
						return err
					}

					node.Parent.Size = len(ds)

					for _, d := range ds {
						data := map[string]map[string]interface{}{k: d}
						node.Data = append(node.Data, data)
					}
				} else {
					for i := 0; i < node.Parent.Size; i++ {
						d, err := findOne(ctx, k, v, i, head, node, db)
						if err != nil {
							return err
						}

						node.Data = append(node.Data, d)
					}
				}
			} else { //对象元素
				d, err := findOne(ctx, k, v, index, head, node, db)
				if err != nil {
					return err
				}

				node.Data = append(node.Data, d)
			}
		}
	}

	return nil
}

//解析数组
func parseArray(ctx context.Context, isKeyArray, index int, k string, v *orderedmap.OrderedMap,
	head, node *ParseTree, db *Client) error {
	child := ParseTree{
		Index:  index,
		Parent: node,
	}

	node.Children = append(node.Children, &child)

	//是否有 count
	if cntTmp, hasCount := v.Get("count"); hasCount {
		count, _ := cntTmp.(float64)
		node.SQLCount = int(count)
		v.Delete("count")
	}

	//是否有 join
	if joinTmp, hasJoin := v.Get("join"); hasJoin {
		joins, err := getJoins(joinTmp)
		if err != nil {
			return err
		}

		node.Joins = joins
		v.Delete("join")
	}

	err := ParseNode(ctx, v, index, head, &child, db)
	if err != nil {
		return err
	}

	if isKeyArray == IsArrayField { //数组字段提取
		if node.FieldData == nil {
			node.FieldData = make([][]interface{}, node.Parent.Size)
		}

		node.FieldData[index], _ = getFieldArray(k, &child)
	}

	return nil
}

//获取sub map
func getSubMap(req *orderedmap.OrderedMap, key string) (*orderedmap.OrderedMap, bool) {
	tmp, ok := req.Get(key)
	if !ok {
		return nil, false
	}

	v, ok := tmp.(orderedmap.OrderedMap)
	if !ok {
		return nil, false
	}

	return &v, true
}

//获取兄弟节点
func getSliblingNode(key string, node *ParseTree) *ParseTree {
	slibling := ParseTree{
		Key:    key,
		First:  node.First,
		Prev:   node,
		Parent: node.Parent,
	}

	if slibling.First == nil {
		slibling.First = node
	}

	node.Next = &slibling
	return &slibling
}

//解析join
func parseJoin(req *orderedmap.OrderedMap, head, node *ParseTree) error {
	joins := node.Parent.Joins
	for _, k := range req.Keys() {
		join, ok := joins[k]
		if ! ok {
			continue
		}

		tmp, ok := req.Get(k)
		if !ok {
			continue
		}

		v, ok := tmp.(orderedmap.OrderedMap)
		if !ok {
			continue
		}

		joinedTableField, ok := v.Get(join.Field)
		if !ok {
			return fmt.Errorf("not find joined field")
		}

		fmt.Println(v)
		fmt.Println(join)
		fmt.Println(joinedTableField)
	}
	return nil
}

//是否数组，字段提取数组
func isKeyArray(key string) int {
	l := len(key)
	if l < 2 {
		return IsArrayFalse
	}

	last := key[l-2 : l]

	if last != "[]" {
		return IsArrayFalse
	}

	if strings.Index(key, "-") != -1 {
		return IsArrayField
	}

	return IsArrayTrue
}

//数组字段提取结果获取
func getFieldArray(k string, node *ParseTree) ([]interface{}, error) {
	ret := []interface{}{}

	lk := len(k)
	var pathStr string
	if lk > 2 {
		pathStr = k[0 : lk-2]
	} else {
		pathStr = k
	}

	path := strings.Split(pathStr, "-")

	err := realGetFieldArray(path, node, &ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func realGetFieldArray(path []string, node *ParseTree, result *[]interface{}) error {
	pLen := len(path)

	var i int
	next := node

	for {
		cd := path[i]
		if cd == "" {
			return fmt.Errorf("path is invalid")
		}

		if next.Key != cd {
			next = next.Next
			if next == nil {
				//路径错误，按照路径未找到指定的关联引用
				return fmt.Errorf("not find ssociated path")
			}
			continue
		}

		i++
		if i == pLen-1 {
			break
		}

		if len(next.Children) == 0 {
			//路径中断，还未找到整个关联引用
			return fmt.Errorf("associated path is error")
		}

		for _, child := range next.Children {
			return realGetFieldArray(path[1:], child, result)
		}
	}

	field := path[pLen-1]
	for _, data := range next.Data {
		if d, ok := data[next.Key][field]; ok {
			if !inArray(d, result) {
				*result = append(*result, d)
			}
		}
	}

	return nil
}

//查询一个记录
func findOne(ctx context.Context, table string,
	where *orderedmap.OrderedMap, index int,
	head, node *ParseTree, db *Client) (map[string]map[string]interface{}, error) {
	statement := genStatement(table, where, index, head, node)

	//引用赋值异常，查询结果直接置为 nil
	if statement == nil {
		return nil, nil
	}

	d, err := db.FindOneMap(ctx, statement)
	if err != nil {
		return nil, err
	}

	return map[string]map[string]interface{}{table: d}, nil
}

//查询多条记录
func findAll(ctx context.Context, table string,
	where *orderedmap.OrderedMap, index int,
	head, node *ParseTree, db *Client) ([]map[string]interface{}, error) {
	statement := genStatement(table, where, index, head, node)

	//引用赋值异常，查询结果直接置为 nil
	if statement == nil {
		return nil, nil
	}

	d, err := db.FindAllMaps(ctx, statement)
	if err != nil {
		return nil, err
	}

	return d, nil
}

//生成 sql 语法
func genStatement(table string, where *orderedmap.OrderedMap,
	index int, head, node *ParseTree) *Statement {
	//关联引用赋值
	newWhere, err := associatedAssignments(where, index, head, node)
	if err != nil {
		return nil
	}

	statement := NewDbStatement()
	statement.SetTableName(table)
	statement.Where(newWhere)

	return statement
}

//关联引用赋值
func associatedAssignments(orderMap *orderedmap.OrderedMap,
	index int, head, node *ParseTree) (*orderedmap.OrderedMap, error) {
	var newWhere *orderedmap.OrderedMap

	//判断是否包含关联引用
	for _, key := range orderMap.Keys() {
		if key == "" {
			continue
		}
		if ok, _ := isAssociated(key); ok {
			newWhere = orderedmap.New()
			break
		}
	}

	//没有引用赋值，则直接返回原条件 map
	if newWhere == nil {
		return orderMap, nil
	}

	for _, key := range orderMap.Keys() {
		if key == "" {
			continue
		}

		val, exist := orderMap.Get(key)
		if !exist {
			continue
		}

		//判断是否引用
		if ok, newKey := isAssociated(key); ok {
			associated, isString := val.(string)
			if !isString {
				return nil, fmt.Errorf("associated value is invalid")
			}

			newVal, err := associatedAssignment(associated, index, head, node)
			if err != nil {
				return nil, err
			}

			newWhere.Set(newKey, newVal)
		} else {
			newWhere.Set(key, val)
		}
	}

	return newWhere, nil
}

//元素的关联引用赋值
func associatedAssignment(associated string,
	index int, head, node *ParseTree) (interface{}, error) {
	path := strings.Split(associated, "/")
	pLen := len(path)
	field := path[pLen-1]

	isKeyArr := isKeyArray(field)

	var i int
	next := head
	if path[0] == "" { //相对路径，从当前容器开始
		next = node.First
		i = 1
	}

	//路径错误，按照路径未找到指定的关联引用
	if next == nil {
		return nil, fmt.Errorf("ssociated path is invalid")
	}

	for {
		cd := path[i]
		if cd == "" {
			i++
			continue
		}

		if next.Key != cd {
			next = next.Next
			if next == nil {
				//路径错误，按照路径未找到指定的关联引用
				return nil, fmt.Errorf("not find ssociated path")
			}
			continue
		}

		i++
		if (isKeyArr != IsArrayField && i == pLen-1) ||
			(isKeyArr == IsArrayField && i == pLen) {
			break
		}

		if len(next.Children) == 0 {
			//路径中断，还未找到整个关联引用
			return nil, fmt.Errorf("associated path is error")
		}

		if len(next.Children) == 1 {
			next = next.Children[0]
		} else {
			childIndex := findChildIndex(node, next, index)
			if childIndex == -1 { //关联引用值不在本节点的所有父层级
				return nil, fmt.Errorf("associated is not in parents layer")
			} else {
				next = next.Children[childIndex]
			}
		}
	}

	if next == nil {
		return nil, fmt.Errorf("associated path is end")
	}

	if (isKeyArr != IsArrayField && len(next.Data) == 0) ||
		(isKeyArr == IsArrayField && len(next.FieldData) == 0) {
		return nil, fmt.Errorf("associated data is empty")
	}

	index = findChildIndex(node, next, index)
	if index == -1 { //关联引用值不在本节点的所有父层级
		return nil, fmt.Errorf("associated is not in parents layer")
	}

	if isKeyArr != IsArrayField {
		data := next.Data[index][next.Key]
		if _, ok := data[field]; !ok {
			return nil, fmt.Errorf("not find associated field")
		}

		return data[field], nil
	} else {
		if len(next.FieldData[index]) == 0 {
			return nil, fmt.Errorf("associated data is empty")
		}

		return next.FieldData[index], nil
	}
}

//从 node 回溯找到子路径 index
func findChildIndex(node, next *ParseTree, index int) int {
	//首先判断是否与 next 为平行节点
	prev := node.Prev
	for {
		if prev == nil {
			break
		}

		if prev == next {
			return index
		}

		prev = prev.Prev
	}

	for {
		parent := node.Parent
		if parent == nil {
			return -1
		}

		prev := parent.Prev
		for {
			if prev == nil {
				node = parent
				break
			}

			if prev == next {
				return node.Index
			}

			prev = prev.Prev
		}
	}
}

//判断是否关联引用
func isAssociated(key string) (bool, string) {
	l := len(key)
	if l == 0 {
		return false, ""
	}

	last := key[l-1 : l]
	if last == "@" {
		return true, key[0 : l-1]
	}

	return false, ""
}

//提取 joins
func getJoins(joinTmp interface{}) (map[string]*Join, error) {
	joinStrs, _ := joinTmp.(string)
	if joinStrs == "" {
		return nil, nil
	}

	joinStrArr := strings.Split(joinStrs, ",")

	joins := map[string]*Join{}
	for _, joinStr := range joinStrArr {
		join, err := getJoin(joinStr)
		if err != nil {
			return nil, err
		}

		joins[join.Table] = join
	}

	return joins, nil
}

func getJoin(joinStr string) (*Join, error) {
	joinFields := strings.Split(joinStr, "/")

	join := Join{}

	switch joinFields[0] {
	case "<":
		join.Type = "LEFT JOIN"
	case ">":
		join.Type = "RIGHT JOIN"
	case "&":
		join.Type = "INNER JOIN"
	case "FULL":
		join.Type = "FULL JOIN"
	case "!":
		join.Type = "OUTER JOIN"
	default:
		return nil, fmt.Errorf("join type is invalid")
	}

	join.Table = joinFields[1]
	join.Field = joinFields[2]

	return &join, nil
}

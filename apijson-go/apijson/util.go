package apijson

import (
	"github.com/iancoleman/orderedmap"
	"reflect"
)

func encodeResult(head *ParseTree, data *orderedmap.OrderedMap) {
	if head == nil {
		return
	}

	if head.IsArray {
		if head.IsFieldArray {
			data.Set(head.Key, &head.FieldData[0])
		} else {
			sub := []*orderedmap.OrderedMap{}
			data.Set(head.Key, &sub)
			encodeArrayResult(head.Children[0], head.Size, &sub)
		}
	} else {
		data.Set(head.Key, head.Data[0][head.Key])
	}

	encodeResult(head.Next, data)
	return
}

func encodeArrayResult(head *ParseTree, size int, datas *[]*orderedmap.OrderedMap) {
	for {
		if head == nil {
			break
		}

		if size > 0 {
			if len(*datas) == 0 {
				*datas = make([]*orderedmap.OrderedMap, size)
			}

			if head.IsArray {
				if head.IsFieldArray {
					for i := 0; i < size; i++ {
						if (*datas)[i] == nil {
							(*datas)[i] = orderedmap.New()
						}
						(*datas)[i].Set(head.Key, &head.FieldData[i])
					}
				} else {
					for i := 0; i < size; i++ {
						sub := []*orderedmap.OrderedMap{}
						if (*datas)[i] == nil {
							(*datas)[i] = orderedmap.New()
						}
						(*datas)[i].Set(head.Key, &sub)

						if head.Children == nil {
							continue
						}

						encodeArrayResult(head.Children[i], head.Size, &sub)
					}
				}
			} else {
				for i := 0; i < size; i++ {
					if (*datas)[i] == nil {
						(*datas)[i] = orderedmap.New()
					}

					if head.Data != nil {
						(*datas)[i].Set(head.Key, head.Data[i][head.Key])
					} else {
						(*datas)[i].Set(head.Key, nil)
					}

				}
			}
		}

		head = head.Next
	}

	return
}

func inArray(need interface{}, needArr *[]interface{}) bool {
	needVal := reflect.ValueOf(need)
	if needVal.Kind() == reflect.Ptr {
		need = needVal.Elem().Interface()
	}

	for _, v := range *needArr {
		vVal := reflect.ValueOf(v)
		if vVal.Kind() == reflect.Ptr {
			v = vVal.Elem().Interface()
		}

		if need == v {
			return true
		}
	}

	return false
}

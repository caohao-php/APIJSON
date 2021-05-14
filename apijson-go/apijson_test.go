package main

import (
	"apijson/apijson"
	"context"
	"fmt"
	"testing"
)

func Test_Apijson_Analyze(t *testing.T) {
	/*	reqbody := []byte(`
		{
			"a": {
				"id>": 0
			},
			"b": {
				"id>": 0
			},
			"[]": {
				"c": {
					"id>":0
				},
				"d": {
					"id>":0
				},
				"[]": {
					"e": {
						"id<":5710
					},
					"f": {
						"id>":0
					},
					"[]": {
						"h": {
							"id>":0,
							"task_key@":"/[]/c/task_key"
						},
						"l": {
							"id>":0
						}
					}
				}
			},
			"pp[]": {
				"x": {
					"id>":0
				},
				"y": {
					"id>":3
				}
			}
		}
		`)

	*/
	reqbody := []byte(`
		{
			"Moment": {
				"id>": 0
			},
			"apijson_user": {
				"id@": "Moment/userId"
			},
			"apijson_user-id[]": {
				"apijson_user": {
					"id<=": 82003,
					"@order": "id-"
				}
			},
			"[]": {
				"Comment": {
					"id<": 1000,
					"userId{}@": "apijson_user-id[]"
				},
				"apijson_user": {
					"id@": "[]/Comment/userId"
				}
			}
		}
		`)

	ctx := context.Background()
	dbName := `root:apijson@tcp(apijson.cn:3306)/sys?timeout=1s&parseTime=true&charset=utf8&loc=Local`
	out, err := apijson.Parse(ctx, dbName, reqbody)
	fmt.Println(string(out))
	fmt.Println(err)
}

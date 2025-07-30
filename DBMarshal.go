package db

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/pretty"
	"github.com/tidwall/sjson"
)

func MarshalDB(db DB, indent bool) ([]byte, string, error) {
	jsonBytes, err := json.Marshal(db)
	if err != nil {
		return nil, "", err
	}
	for _, v := range DBTypeAll {
		if v != db.Type {
			if gjson.GetBytes(jsonBytes, string(v)).Exists() {
				jsonBytes, err = sjson.DeleteBytes(jsonBytes, string(v))
				if err != nil {
					return nil, "", err
				}
			}
		}
	}
	if indent {
		return pretty.Pretty(jsonBytes), string(pretty.Pretty(jsonBytes)), nil
	} else {
		return jsonBytes, string(jsonBytes), nil
	}
}

func MarshalDBList(dbList []DB, indent bool) ([]byte, string, error) {
	var jsonList []string
	for _, v := range dbList {
		_, jsonString, err := MarshalDB(v, false)
		if err != nil {
			return nil, "", err
		}
		jsonList = append(jsonList, jsonString)
	}
	jsonBytes := []byte("[" + strings.Join(jsonList, ",") + "]")
	if indent {
		return pretty.Pretty(jsonBytes), string(pretty.Pretty(jsonBytes)), nil
	} else {
		return jsonBytes, string(jsonBytes), nil
	}
}

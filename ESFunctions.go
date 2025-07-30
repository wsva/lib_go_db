package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	wl_http "github.com/wsva/lib_go/http"

	"github.com/elastic/go-elasticsearch/v8/esapi"
)

/*
传入参数request内容样式如下
#skuinfo是索引名
GET /skuinfo/_search

	{
	  "query": {
	    "match_all": {}
	  }
	}

返回的HttpClient不能直接使用，address需要补全host部分
*/
func ESParseRequest(request string) (*wl_http.HttpClient, error) {
	reg := regexp.MustCompile(`^([A-Z]+)\s+([\S]+)$`)
	var method, suburl, bodydata string
	for linenum, line := range strings.Split(request, "\n") {
		if linenum == 0 {
			if !reg.MatchString(line) {
				return nil, errors.New("invalid request first line")
			}
			subMatchList := reg.FindStringSubmatch(line)
			switch subMatchList[1] {
			case "GET":
				method = http.MethodGet
			case "PUT":
				method = http.MethodPut
			case "POST":
				method = http.MethodPost
			case "DELETE":
				method = http.MethodDelete
			default:
				return nil, errors.New("invalid request method")
			}
			suburl = subMatchList[2]
		} else {
			bodydata += line + "\n"
		}
	}

	//预先检查request中的内容是否有语法错误
	reg = regexp.MustCompile(`\s`)
	if reg.ReplaceAllString(bodydata, "") != "" {
		var bodyMap map[string]interface{}
		err := json.Unmarshal([]byte(bodydata), &bodyMap)
		if err != nil {
			return nil, errors.New("invalid request body")
		}
	}

	return &wl_http.HttpClient{
		Address: suburl,
		Method:  method,
		Data:    strings.NewReader(bodydata),
		Timeout: 10,
		HeaderMap: map[string]string{
			"Content-Type": "application/json",
		},
	}, nil
}

// ESGetHitsList comment
func ESGetHitsList(response []byte) ([]interface{}, error) {
	reg := regexp.MustCompile(`"hits"\s+:`)
	findResult := reg.FindAll(response, 2)
	if len(findResult) < 2 {
		return nil, errors.New("key hits not found: " + string(response))
	}
	var jsonMap map[string]interface{}
	err := json.Unmarshal(response, &jsonMap)
	if err != nil {
		return nil, errors.New(err.Error() + string(response))
	}
	return jsonMap["hits"].(map[string]interface{})["hits"].([]interface{}), nil
}

// ESCheckIndexResult comment
func ESCheckIndexResult(reponse []byte) (string, bool) {
	regCreated := regexp.MustCompile(`"result"\s+:\s+"created"`)
	regUpdated := regexp.MustCompile(`"result"\s+:\s+"updated"`)
	if regCreated.Match(reponse) {
		return "created", true
	}
	if regUpdated.Match(reponse) {
		return "updated", true
	}
	return "", false
}

func ESTranslateSql(address, sqlString string) ([]byte, error) {
	bodydata := `{"query":"` + sqlString + `"}`
	client := wl_http.HttpClient{
		Address: address + "/_sql/translate",
		Method:  http.MethodPost,
		Data:    strings.NewReader(bodydata),
		Timeout: 10,
		HeaderMap: map[string]string{
			"Content-Type": "application/json",
		},
	}
	return client.DoRequest()
}

func ESGetReader(data interface{}) (io.Reader, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(data); err != nil {
		return nil, err
	}
	return &buf, nil
}

func ESPrintResponse(resp *esapi.Response, err error) {
	defer resp.Body.Close()
	fmt.Println("==========BEGIN==========")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(body))
}

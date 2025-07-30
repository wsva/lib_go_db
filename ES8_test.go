package db_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	db "github.com/wsva/lib_go_db"
)

func TestESClientMapAdd(t *testing.T) {
	es8 := db.ES8{
		AddressList: []string{"https://10.0.0.1:9200"},
		Username:    "elastic",
		Password:    "Z9-dqdlQGADT=CvCq4=S",
	}
	client, err := es8.Client()
	if err != nil {
		fmt.Println(err)
		return
	}

	//--------------------------------------------
	/*
		ESPrintResponse(client.Info())

		ESPrintResponse(client.Cat.Indices())
	*/
	//--------------------------------------------

	req1 := esapi.CreateRequest{
		Index: "test3",
		//DocumentID: "1",
		Body: strings.NewReader(`{
			"First":  "1",
			"Second": "2"
		}`),
		Pretty: true,
	}
	db.ESPrintResponse(req1.Do(context.Background(), client))

	//--------------------------------------------
	/*
		req2 := esapi.IndexRequest{
			Index:      "test1",
			DocumentID: "1",
			Body: strings.NewReader(`{
				"First":  "2",
				"Second": "3"
			}`),
			Pretty: true,
		}
		ESPrintResponse(req2.Do(context.Background(), client)) */

	//--------------------------------------------

	/* req3 := esapi.UpdateRequest{
		Index:      "test1",
		DocumentID: "1",
		Body: strings.NewReader(`{
			"First":  "4",
			"Second": "5"
		}`),
		Pretty: true,
	}
	ESPrintResponse(req3.Do(context.Background(), client)) */

	//--------------------------------------------

	/* from, size := 0, 1
	req5 := esapi.SearchRequest{
		Index: []string{"test1"},
		Body: strings.NewReader(`{
			"query": {
				"match_all": {}
			}
		}`),
		From:   &from,
		Size:   &size,
		Pretty: true,
	}
	ESPrintResponse(req5.Do(context.Background(), client)) */
}

/* func TestESHttpRequest(t *testing.T) {
	address := "http://10.0.0.31:9200"
	request := `GET /worklog/_search?pretty
	{
		"query": {
			"match": {
				"Status": "normal"
			}
		}
	}
	`
	resp, err := ESDoHttpRequest(address, request)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(resp))
} */

/* func TestTranslateSql(t *testing.T) {
	address := "http://10.0.0.31:9200"
	sql := "select YunWeiRenYuan, count(*) from worklog " +
		"where Status='normal' and ChuLiRiQi='2021-01-01' " +
		"group by YunWeiRenYuan"
	result, err := ESTranslateSql(address, sql)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(result)
}
*/

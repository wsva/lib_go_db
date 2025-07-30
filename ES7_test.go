package db_test

/*
func TestESClientMapAdd(t *testing.T) {
	clientMap := GetESClientMap()
	err := clientMap.Add(ESConfig7{
		Addresses: []string{"http://10.0.0.31:9200"},
	})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(clientMap)
}

func TestESIndex(t *testing.T) {
	config := ESConfig7{
		Addresses: []string{"http://10.0.0.31:9200"},
	}
	result, err := config.Index("test2", "xg_k1HkBvq4KnSlrwhr7", map[string]string{
		"First":  "33333333",
		"Second": "44444444",
	})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(result)
}

func TestESQuery(t *testing.T) {
	config := ESConfig7{
		Addresses: []string{"http://10.0.0.31:9200"},
	}
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"Status": "normal",
			},
		},
	}
	result, err := config.Query("worklog", query)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(result)
}


func TestESHttpRequest(t *testing.T) {
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
}


func TestTranslateSql(t *testing.T) {
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

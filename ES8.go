package db

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	esv8 "github.com/elastic/go-elasticsearch/v8"
)

type ES8 struct {
	AddressList []string `json:"AddressList"`
	Username    string   `json:"Username"`
	Password    string   `json:"Password"`

	c *esv8.Client `json:"-"`
}

func (e *ES8) initClient() error {
	config := esv8.Config{
		Addresses: e.AddressList,
		Username:  e.Username,
		Password:  e.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	client, err := esv8.NewClient(config)
	if err != nil {
		return err
	}
	resp, err := client.Info()
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("test info failed")
	}
	e.c = client
	return nil
}

func (e *ES8) Client() (*esv8.Client, error) {
	if e.c == nil {
		err := e.initClient()
		if err != nil {
			return nil, err
		}
	}
	return e.c, nil
}

/*
a list of index names to search; use _all to perform the operation on all indices.

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"title": "test",
			},
		},
	}
*/
func (e *ES8) search(
	index []string,
	query map[string]interface{},
) (map[string]interface{}, error) {
	if e.c == nil {
		err := e.initClient()
		if err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, fmt.Errorf("encode query error: %v", err)
	}

	resp, err := e.c.Search(
		e.c.Search.WithContext(context.Background()),
		e.c.Search.WithIndex(index...),
		e.c.Search.WithBody(&buf),
		e.c.Search.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return nil, errors.New(resp.String())
	}

	var m map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, errors.New(resp.String())
	}
	return m, nil
}

func (e *ES8) Search(
	index []string,
	query map[string]interface{},
) ([]byte, error) {
	m, err := e.search(index, query)
	if err != nil {
		return nil, err
	}

	var result []interface{}
	for _, hit := range m["hits"].(map[string]interface{})["hits"].([]interface{}) {
		result = append(result, hit.(map[string]interface{})["_source"])
	}
	return json.Marshal(result)
}

func (e *ES8) HitsCount(
	index []string,
	query map[string]interface{},
) (int64, error) {
	m, err := e.search(index, query)
	if err != nil {
		return 0, err
	}
	hits := m["hits"].(map[string]interface{})
	total := hits["total"].(map[string]interface{})
	return int64(total["value"].(float64)), nil
}

/*
create document(automatically create index if not exists)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"title": "test",
			},
		},
	}
*/
func (e *ES8) Create(
	index []string,
	query map[string]interface{},
) error {
	if e.c == nil {
		err := e.initClient()
		if err != nil {
			return err
		}
	}
	return nil
}

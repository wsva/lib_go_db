package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"

	esv7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type ES7 struct {
	AddressList []string `json:"AddressList"`
	Username    string   `json:"Username"`
	Password    string   `json:"Password"`
	CACrtFile   string   `json:"CACrtFile"`

	config *esv7.Config `json:"-"`
	client *esv7.Client `json:"-"`
}

func (e *ES7) initConfig() error {
	config := &esv7.Config{
		Addresses: e.AddressList,
		Username:  e.Username,
		Password:  e.Password,
	}
	if e.CACrtFile != "" {
		contentBytes, err := ioutil.ReadFile(e.CACrtFile)
		if err != nil {
			return err
		}
		config.CACert = contentBytes
	}
	e.config = config
	return nil
}

func (e *ES7) initClient() error {
	if e.config == nil {
		err := e.initConfig()
		if err != nil {
			return err
		}
	}
	client, err := esv7.NewClient(*e.config)
	if err != nil {
		return err
	}
	e.client = client
	return nil
}

/*
Query comment

	query := map[string]interface{}{
		  "query": map[string]interface{}{
			"match": map[string]interface{}{
			  "title": "test",
			},
		  },
		}
*/
func (e *ES7) Query(index string, query map[string]interface{}) ([]interface{}, error) {
	if e.client == nil {
		err := e.initClient()
		if err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}
	res, err := e.client.Search(
		e.client.Search.WithContext(context.Background()),
		e.client.Search.WithIndex(index),
		e.client.Search.WithBody(&buf),
		e.client.Search.WithTrackTotalHits(true),
		e.client.Search.WithPretty(),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, errors.New("response indicates error")
	}
	var result map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result["hits"].(map[string]interface{})["hits"].([]interface{}), nil
}

// Index insert or update
func (e *ES7) Index(esindex, docid string, data interface{}) (interface{}, error) {
	if e.client == nil {
		err := e.initClient()
		if err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(data); err != nil {
		return nil, err
	}
	req := esapi.IndexRequest{}
	if esindex != "" {
		req.Index = esindex
	} else {
		return nil, errors.New("index cannot be empty")
	}
	if docid != "" {
		req.DocumentID = docid
	}
	if data != nil {
		req.Body = &buf
	} else {
		return nil, errors.New("data cannot be empty")
	}
	res, err := req.Do(context.Background(), e.client)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var result map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

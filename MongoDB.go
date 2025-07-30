package db

import (
	"context"
	"encoding/json"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MgoReq struct {
	Database   string                 `json:"Database"`
	Collection string                 `json:"Collection"`
	Filter     interface{}            `json:"Filter"`
	Field      string                 `json:"Field"`
	Data       map[string]interface{} `json:"Data"`
}

/*
URI: "mongodb://root:root@127.0.0.1:27017/admin"

AuthMechanism: the mechanism to use for authentication.
Supported values include "SCRAM-SHA-256", "SCRAM-SHA-1",
"MONGODB-CR", "PLAIN", "GSSAPI", "MONGODB-X509", and "MONGODB-AWS".
This can also be set through the "authMechanism" URI option.
(e.g. "authMechanism=PLAIN"). For more information,

AuthSource: the name of the database to use for authentication.
This defaults to "$external" for MONGODB-X509, GSSAPI, and PLAIN
and "admin" for all other mechanisms.
This can also be set through the "authSource" URI option (e.g. "authSource=otherDb").
*/
type MongoDB struct {
	URI string `json:"URI"`

	Username      string   `json:"Username"`
	Password      string   `json:"Password"`
	AuthMechanism string   `json:"AuthMechanism"`
	AuthSource    string   `json:"AuthSource"`
	Hosts         []string `json:"Hosts"` //["localhost:27017"]

	client *mongo.Client `json:"-"`
}

func (m *MongoDB) initClient() error {
	var co *options.ClientOptions
	if m.URI != "" {
		co = options.Client().ApplyURI(m.URI)
	} else {
		u := "mongodb://" + strings.Join(m.Hosts, ",")
		co = options.Client().ApplyURI(u)
		co.SetAuth(options.Credential{
			Username:      m.Username,
			Password:      m.Password,
			AuthMechanism: m.AuthMechanism,
			AuthSource:    m.AuthSource,
		})
	}
	client, err := mongo.Connect(context.Background(), co)
	if err != nil {
		return err
	}
	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return err
	}
	m.client = client
	return nil
}

func (m *MongoDB) Close() error {
	if m.client == nil {
		return nil
	}
	return m.client.Disconnect(context.Background())
}

func (m *MongoDB) getCollection(req *MgoReq) (*mongo.Collection, error) {
	if m.client == nil {
		err := m.initClient()
		if err != nil {
			return nil, err
		}
	}

	return m.client.Database(req.Database).Collection(req.Collection), nil
}

// Query returns a json list
func (m *MongoDB) Query(req *MgoReq) ([]byte, error) {
	if m.client == nil {
		err := m.initClient()
		if err != nil {
			return nil, err
		}
	}

	coll, err := m.getCollection(req)
	if err != nil {
		return nil, err
	}
	cursor, err := coll.Find(context.Background(), req.Filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	var resultList []map[string]interface{}
	cursor.All(context.Background(), &resultList)
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(resultList)
}

// Query returns a json
func (m *MongoDB) QueryOne(req *MgoReq) ([]byte, error) {
	coll, err := m.getCollection(req)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	err = coll.FindOne(context.Background(), req.Filter).Decode(&result)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

// Query returns a json
func (m *MongoDB) QueryMax(req *MgoReq) ([]byte, error) {
	coll, err := m.getCollection(req)
	if err != nil {
		return nil, err
	}
	cursor, err := coll.Aggregate(context.Background(), []bson.M{{
		"$group": bson.M{
			"_id": "",
			"max": bson.M{"$max": "$" + req.Field},
		}}})
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	cursor.Decode(&result)
	return json.Marshal(result)
}

// InsertOne comment
func (m *MongoDB) InsertOne(req *MgoReq) error {
	coll, err := m.getCollection(req)
	if err != nil {
		return err
	}
	_, err = coll.InsertOne(context.Background(), req.Data)
	if err != nil {
		return err
	}
	return nil
}

// Update comment
func (m *MongoDB) Update(req *MgoReq) (int64, error) {
	coll, err := m.getCollection(req)
	if err != nil {
		return 0, err
	}
	result, err := coll.UpdateMany(context.Background(), req.Filter, req.Data)
	if err != nil {
		return result.ModifiedCount, err
	}
	return result.ModifiedCount, nil
}

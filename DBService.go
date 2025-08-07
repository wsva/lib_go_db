package db

import (
	"errors"
	"sync"
)

/*
*************************execute request*************************
 */
type SqlReq struct {
	ID    string
	Query string
	Args  []any
	Limit int
}

func NewSqlReq(id, sqlstr string, limit int, args ...any) *SqlReq {
	return &SqlReq{
		ID:    id,
		Query: sqlstr,
		Args:  args,
		Limit: limit,
	}
}

/*
*************************execute request (batch)*************************
 */
type SqlReqBatch struct {
	IDList []string `json:"IDList"`
	SqlStr string   `json:"SqlStr"`
	Limit  int      `json:"Limit"`
}

func (s *SqlReqBatch) SqlReqList() []*SqlReq {
	var result []*SqlReq
	for k := range s.IDList {
		result = append(result, NewSqlReq(s.IDList[k], s.SqlStr, s.Limit))
	}
	return result
}

/*
*************************result of query*************************
 */

/*
ID indicates which database does the result come from
*/
type QueryResult struct {
	ID     string
	Result []any
	ErrMsg string
}

/*
*************************result of query (batch)*************************
 */
type QueryResultList struct {
	Data []QueryResult
	Lock sync.Mutex
	WG   *sync.WaitGroup
}

func NewQueryResultList() *QueryResultList {
	return &QueryResultList{
		WG: &sync.WaitGroup{},
	}
}

/*
*************************all databases*************************
 */
//ID as key
type DBMap map[string]*DB

func (m DBMap) Query2MapList(req *SqlReq, limit int) ([]any, error) {
	db, ok := m[req.ID]
	if !ok {
		return nil, errors.New("no database found, ID: " + req.ID)
	}
	return db.Query2MapList(limit, req.Query, req.Args...)
}

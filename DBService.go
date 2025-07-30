package db

import (
	"errors"
	"sync"
)

/*
*************************执行请求*************************
 */
type SqlReq struct {
	ID     string `json:"ID"`
	SqlStr string `json:"SqlStr"`
	Limit  int    `json:"Limit"`
}

func NewSqlReq(id, sqlstr string, limit int) *SqlReq {
	return &SqlReq{
		ID:     id,
		SqlStr: sqlstr,
		Limit:  limit,
	}
}

/*
*************************执行请求（批量）*************************
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
*************************查询结果*************************
 */

/*
ID是为了标记查询结果是哪个数据库的查询结果
*/
type QueryResult struct {
	ID     string        `json:"ID"`
	Result []interface{} `json:"Result"`
	ErrMsg string        `json:"ErrMsg"`
}

/*
*************************查询结果（批量）*************************
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
*************************服务端所有DB*************************
 */
//ID as key
type DBMap map[string]*DB

func (m DBMap) Query2MapList(req *SqlReq, limit int) ([]interface{}, error) {
	db, ok := m[req.ID]
	if !ok {
		return nil, errors.New("no database found, ID: " + req.ID)
	}
	return db.Query2MapList(req.SqlStr, limit)
}

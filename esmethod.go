package elasticsearch7

import (
	"context"

	"github.com/olivere/elastic/v7"
)

var (
	EsClient *elastic.Client
	err      error
)

func init() {
	//链接es
	EsClient, err = elastic.NewClient(elastic.SetURL("http://127.0.0.1:9400"), elastic.SetSniff(false))
	if err != nil {
		// Handle error
		panic(err)
		return
	}
}

// 判断索引是否存在
func ExistsIndex(IndexName string) (bool, error) {
	exists, err := EsClient.IndexExists(IndexName).Do(context.Background())
	return exists, err
}

// 创建索引
func CreateIndex(IndexName string) (bool, error) {

	// Create a new index.
	//mapping关系映射
	mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
			"properties":{
				"user":{
					"type":"keyword"
				},
				"message":{
					"type":"text"
				},
                "retweets":{
                    "type":"long"
                },
				"tags":{
					"type":"keyword"
				},
				"location":{
					"type":"geo_point"
				},
				"suggest_field":{
					"type":"completion"
				}
			}
	}
}
`
	createIndex, err := EsClient.CreateIndex(IndexName).BodyString(mapping).Do(context.Background())
	if err != nil {
		// Handle error
		panic(err)
	}
	return createIndex.Acknowledged, err
}

// 修改信息
func UpdateDoc(IndexName string) (*elastic.BulkIndexByScrollResponse, error) {
	script := elastic.NewScript("ctx._source.user = params.user").Param("user", "马蛋儿")
	response, err := EsClient.UpdateByQuery().
		Index(IndexName).Query(elastic.NewMatchQuery("user", "马蛋")).Script(script).Do(context.Background())
	if err != nil {
		// 处理错误
		panic(err)
	}
	return response, err
}

// 删除索引单个文档
func DelDoc(IndexName string) (*elastic.BulkIndexByScrollResponse, error) {
	do, err := EsClient.DeleteByQuery().Index(IndexName).Query(elastic.NewMatchQuery("id", 0)).Do(context.Background())
	if err != nil {
		panic(err)
	}
	return do, err
}

// // // 根据条件查询文档信息
func SelDoc(IndexName string) (*elastic.GetResult, error) {
	do, err := EsClient.Get().Index(IndexName).Id("1").Do(context.Background())
	if err != nil {
		return nil, nil
	}
	return do, err
}

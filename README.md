# elasticSearchJavaDemo

### guid
[web](https://www.elastic.co/guide/index.html)

### 端口
```
Elasticsearch支持Http类型的Restful风格API请求，需要打开9200端口。Elasticsearch服务会监听两个端口9200和9300，
9200提供Http Restful访问，
9300是tcp通讯端口，集群间和TCPClient都走的它
```

### 项目代码：
CRUD 相关。


### 批量导入数据 
```$xslt
文件在 resources 里面


curl -H "Content-Type: application/json" -XPOST "localhost:9200/bank/_bulk?pretty&refresh" --data-binary "@testAccounts.json"
curl "localhost:9200/_cat/indices?v"

测试聚合 aggregation
```


---

入门指导：
﻿https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started-query-document.html
 
curl 基础知识
```
curl 

-h来查看请求参数的含义 ,请求头
-v 显示请求的信息 
-X 选项指定其它协议

```

一、  查看健康状态
```
GET /_cat/health?v

curl localhost:9200/_cat/health?v
```

二、  查看节点状态 
```
 GET /_cat/nodes?v
curl localhost:9200/_cat/nodes?v
```

三、  查看目录 indices
```
GET /_cat/indices?v
curl localhost:9200/_cat/indices?v


```
四、 创建目录
```
PUT /customer?pretty
GET /_cat/indices?v
curl -XPUT localhost:9200/customer?pretty
curl localhost:9200/_cat/indices?v
```
五、 往 index里面插入 document
```
PUT /customer/_doc/1?pretty
{
  "name": "John Doe"
}

curl -XPUT -H "Content-Type:application/json" localhost:9200/customer/_doc/2?pretty -d '
{
  "name": "JebLin",
   "age":25,
    "address":"beijing"
}'

上面的 1 就是 id，如果重复操作，那么会覆盖 update，如果改了id，那么就会新创建 insert。
若不指定id，跟下面一样，那么es会自动创建id。
curl -XPUT -H "Content-Type:application/json" localhost:9200/customer/_doc?pretty -d '


---
查询插入结果：
GET /customer/_doc/1?pretty
curl localhost:9200/customer/_doc/1?pretty
```
六、 删除 index
```
删除某个id
Delete /customer/_doc/2
curl -XDELETE localhost:9200/customer/_doc/2
---
删除整个 index
Delete /customer

curl -XDELETE localhost:9200/customer
curl localhost:9200/_cat/indices?v
```
七、批量操作
```
POST /customer/_bulk?pretty

每一个操作要两个json串，语法如下：
{"action": {"metadata"}}
{"data"}
bulk api对json的语法，有严格的要求，每个json串不能换行，只能放一行，
同时一个json串和一个json串之间，必须有一个换行


curl -XPUT -H "Content-Type:application/json" localhost:9200/customer/_bulk?pretty -d '
{"update":{"_id":"1"}} 
{"doc":{"name": "JebLin"}}
{"update":{"_id":"2"}}
{"doc":{"name": "JebLin2"}}
'
```


八、批量导入
```
切换到导入文件的目录下：

curl -H "Content-Type: application/json" -XPOST "localhost:9200/bank/_bulk?pretty&refresh" --data-binary "@testAccounts.json"
curl "localhost:9200/_cat/indices?v"

---
MOMOdeMacBook-Pro:Downloads momo$ curl "localhost:9200/_cat/indices?v"
health status index    uuid                   pri rep docs.count docs.deleted store.size pri.store.size
yellow open   bank     JJGhSSAgTkSs_w1X85clxg   1   1       1000            0    414.2kb        414.2kb
yellow open   customer SLa1MRSUTUWjimsp9HKsmA   1   1          3            1       14kb           14kb
```

九、 查询 
response含义
```
{
  "took" : 63, (耗时）
  "timed_out" : false, （是否超时）
  "_shards" : {
    "total" : 5, (查询总碎片 shard）
    "successful" : 5, （查询成功的碎片 shard）
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : { （命中结果）
    "total" : {
        "value": 1000, （命中了了多少个 document）
        "relation": "eq" （hitcount = 1000) ,equals
    },
    "max_score" : null,
    "hits" : [ {   （命中列表）
      "_index" : "bank", （index）
      "_type" : "_doc", （type类型）
      "_id" : "0", （document 的 ID）
      "sort": [0], (不指定的话，就是按 score 排序）
      "_score" : null, （分数）
      "_source" : {"account_number":0,"balance":16623,"firstname":"Bradshaw","lastname":"Mckenzie","age":29,"gender":"F","address":"244 Columbus Place","employer":"Euron","email":"bradshawmckenzie@euron.com","city":"Hobucken","state":"CO"}
    }, {
      "_index" : "bank",
      "_type" : "_doc",
      "_id" : "1",
      "sort": [1],
      "_score" : null,
      "_source" : {"account_number":1,"balance":39225,"firstname":"Amber","lastname":"Duke","age":32,"gender":"M","address":"880 Holmes Lane","employer":"Pyrami","email":"amberduke@pyrami.com","city":"Brogan","state":"IL"}
    }, ...
    ]
  }
}
```


#### 1. match_all
```
GET /bank/_search
{
  "query": { "match_all": {} },
  "from": 10,
  "size": 10
}

1. index + count + sort
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "query": { "match_all": {} },
  "from": 10,
  "size": 10,
  "sort": { "balance": { "order": "desc" } }
}
'
2. 只出来特定字段

curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
 "query": { "match_all": {} },
 "_source": ["account_number", "balance"]
}
'
```

#### 2. match
```
GET /bank/_search
{
  "query": { "match": { "address": "mill lane" } }
}

curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "query": { "match": { "address": "mill lane" } }
}
'
---
加上 boolean ,must == and ， should == or
GET /bank/_search

curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "query": {
    "bool": {
    	"should":[
    		{"match":{"account_number":"472"}},
    		{"match":{"address":"mill"}}
    	],
    }
  }
}
'
---
must_not ,既不是也不是，下面例子排除掉 employer 是 Comverges 与 Pheast 的
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "query": {
    "bool": {
    	"should":[
    		{"match":{"account_number":"472"}},
    		{"match":{"address":"mill"}}
    	],
    	"must_not":[
    		{"match":{"employer":"Comverges"}},
    		{"match":{"employer":"Pheast"}}
    	]
    }
  }
}
' 
```
#### 3. filter
```
GET /bank/_search
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "query": {
    "bool": {
      "must": { "match_all": {} },
      "filter": {
        "range": {
          "balance": {
            "gte": 20000,
            "lte": 30000
          }
        }
      }
    }
  }
}
'
```

#### 4. 聚合aggregation  (group by)
```
GET /bank/_search
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "size": 0, （设置为0，是为了不展示 hit内容，只看 aggregation 结果）
  "aggs": {
    "group_by_state": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
'
类似：
SELECT state, COUNT(*) FROM bank GROUP BY state ORDER BY COUNT(*) DESC LIMIT 10;
结果字段为：
key（组名） 与 doc_count （组内doc数量）

---
聚合结果再操作：
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "size": 0,
  "aggs": {
    "group_by_state": {
      "terms": {
        "field": "state.keyword"
      },
      "aggs": {
        "average_balance": {
          "avg": {
            "field": "balance"
          }
        }
      }
    }
  }
}
'

结果字段为：
key（组名） 与 doc_count （组内doc数量） + 自定义的 average_balance

---
聚合结果排序：
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "size": 0,
  "aggs": {
    "group_by_state": {
      "terms": {
        "field": "state.keyword",
        "order": {
          "average_balance": "desc"
        }
      },
      "aggs": {
        "average_balance": {
          "avg": {
            "field": "balance"
          }
        }
      }
    }
  }
}
'

--- 
range 根据范围分组：
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "size": 0,
  "aggs": {
    "group_by_age": {
      "range": {
        "field": "age",
        "ranges": [
          {
            "from": 20,
            "to": 30
          },
          {
            "from": 30,
            "to": 40
          },
          {
            "from": 40,
            "to": 50
          }
        ]
      }
    }
  }
}
'
匹配结果字段有：
key,from,to,doc_count
---
在匹配结果，再把结果根据性别分开，分开后的结果，再继续操作，求 balance的平均值
curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
{
  "size": 0,
  "aggs": {
    "group_by_age": {
      "range": {
        "field": "age",
        "ranges": [
          {
            "from": 20,
            "to": 30
          },
          {
            "from": 30,
            "to": 40
          },
          {
            "from": 40,
            "to": 50
          }
        ]
      },
      "aggs": {
        "group_by_gender": {
          "terms": {
            "field": "gender.keyword"
          },
          "aggs": {
            "average_balance": {
              "avg": {
                "field": "balance"
              }
            }
          }
        }
      }
    }
  }
}
'

```

#### warning :
1. 一个 index 只能有一个 type 否则插入另一个 type 的时候，会报异常
```
java.lang.IllegalArgumentException: Rejecting mapping update to [test] as the final mapping would have more than 1 type: [_doc, testType]
```

2. 所以 GET 的时候，可以省略 Type
```
GET index/type/search?pretty
=
GET index/search?pretty
```


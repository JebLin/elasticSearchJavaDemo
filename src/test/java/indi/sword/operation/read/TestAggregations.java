package indi.sword.operation.read;

import indi.sword.operation.TestBankBase;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.junit.Test;

/**
 * 聚合框架有助于根据搜索查询提供数据。它是基于简单的构建块也称为整合，
 * 整合就是将复杂的数据摘要有序的放在一块。聚合可以被看做是从一组文件中获取分析信息的一系列工作的统称。
 * 聚合的实现过程就是定义这个文档集的过程
 *
 * @author jeb_lin
 * 下午3:35 2019/5/11
 */
public class TestAggregations extends TestBankBase {
    @Test
    public void testAggregations() {
        /*
             同：
                curl -XPOST -H "Content-Type:application/json" localhost:9200/bank/_search?pretty -d '
                {
                  "size": 0,
                  "aggs": {
                    "group_by_state": {
                      "terms": {
                        "field": "state.keyword"
                      }
                    }
                  }
                }
                '
         */
        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(0)
                .addAggregation(AggregationBuilders.terms("group_by_state").field("state.keyword")).get();
        Terms aggregateTerm = searchResponse.getAggregations().get("group_by_state");
        Terms aggregateAveBalanceTerm = searchResponse.getAggregations().get("average_balance");

        System.out.println(aggregateTerm);
        System.out.println(aggregateAveBalanceTerm);

        System.out.println(searchResponse.getAggregations().get("agg"));
        /*
            计算文档数错误
            doc_count_error_upper_bound :
                这个表明在最坏情况下，有多少个文档个数的一个值被遗漏了。这就是（文档数错误上界）
         */
    }

}

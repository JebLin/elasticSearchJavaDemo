package indi.sword.operation.read;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import indi.sword.operation.TestBase;
import indi.sword.util.EsUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static indi.sword.util.EsUtil.*;

/**
 * @author jeb_lin
 * 上午11:40 2019/5/9
 */
public class TestGet extends TestBase {

    /**
     * Get API
     */
    @Test
    public void testGetApi() {
        /*
            索引、类型、_id
         */
        GetResponse getResponse = client.prepareGet(INDEX, TYPE, "hvuommoBqn4bnQeoc5BC").get();
        System.out.println(getResponse);
        System.out.println("---");
        Map<String, Object> map = getResponse.getSource();
        Set<String> keySet = map.keySet();
        for (String str : keySet) {
            Object o = map.get(str);
            System.out.println(o.toString());
        }

    }


    /**
     * 一次获取多个文档
     */
    @Test
    public void TestMultiGetApi() {
        MultiGetResponse responses = client.prepareMultiGet()
                .add(INDEX, TYPE, "hfuommoBqn4bnQeoE5Da") //一个ID的方式
                .add(INDEX, TYPE, "gfujmmoBqn4bnQeoVZDf", "1gfujmmoBqn4bnQeoVZDf")//多个ID的方式
                .add("anotherIndex", "anotherType", "idInAnotherIndex") //从另一个索引里面获取
                .get();
        for (MultiGetItemResponse itemResponse : responses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String source = response.getSourceAsString(); //_source
                JSONObject jsonObject = JSON.parseObject(source);
                Set<String> sets = jsonObject.keySet();
                for (String str : sets) {
                    System.out.println("key -> " + str + ",value -> " + jsonObject.get(str));
                    System.out.println("===============");
                }
            }
        }
    }

    @Test
    public void testSearchApi() {
        SearchResponse response = client.prepareSearch(INDEX).setTypes(TYPE)
                .setQuery(QueryBuilders.matchQuery("user", "AA testCreateBulkProcessor")).get();
        System.out.println("---" + response);
        SearchHit[] hits = response.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            String json = hits[i].getSourceAsString();
            JSONObject object = JSON.parseObject(json);
            Set<String> strings = object.keySet();
            for (String str : strings) {
                System.out.println(object.get(str));
            }
        }
    }


    /**
     一般的搜索请求都时返回一页的数据，无论多大的数据量都会返回给用户，
     Scrolls API 可以允许我们检索大量的数据（甚至是全部数据）。
     Scroll API允许我们做一个初始阶段搜索页并且持续批量从ElasticSearch里面拉去结果知道结果没有剩下。
     Scroll API的创建并不是为了实时的用户响应，而是为了处理大量的数据。

     */
    /**
     * 滚动查询
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testScrollApi() throws ExecutionException, InterruptedException {
        Map<String, Object> query = new HashMap<>();
        query.put("name", "JebLin");

        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        initMustQuery(mustQuery, query);
//        initRangeQuery(mustQuery, "age", 9, 13, true, true);

        int maxSize = 3;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(mustQuery)
                .setScroll(new TimeValue(60000)) //为了使用scroll，初始搜索请求应该在查询中指定scroll参数，告诉ElasticSearch需要保持搜索的上下文环境多长时间
                .setSize(maxSize);
        Map<String, SortOrder> sortOrderMap = new HashMap<>();
        sortOrderMap.put("age", SortOrder.DESC);
        sortOrderMap.put("height", SortOrder.DESC);

        initSortOrder(searchRequestBuilder, sortOrderMap);

        SearchResponse response = searchRequestBuilder.get();
        System.out.println(response);
        System.out.println("===============");
        System.out.println("===============");
        System.out.println("hit length -> " + response.getHits().getHits().length);
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                String json = hit.getSourceAsString();
                System.out.println(json);
//                JSONObject object = JSON.parseObject(json);
//                Set<String> strings = object.keySet();
//                for (String str : strings) {
//                    System.out.println(object.get(str));
//                }
                System.out.println(hit.getSortValues()[0]);
            }
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().get();
        } while (response.getHits().getHits().length != 0 && response.getHits().getHits().length < maxSize);


        /*
            虽然滚动时间已过，搜索上下文会自动被清除，但是一直保持滚动代价会很大，所以当我们不在使用滚动时要尽快使用Clear-Scroll API进行清除。
         */
        ClearScrollRequestBuilder clearBuilder = client.prepareClearScroll();
        clearBuilder.addScrollId(response.getScrollId());
        ClearScrollResponse scrollResponse = clearBuilder.get();
        System.out.println("clearOK ? ：" + scrollResponse.isSucceeded());

    }


    @Test
    public void testFilter() throws ExecutionException, InterruptedException {
        Map<String, Object> query = Maps.newHashMap();
        query.put("name", "JebLin");
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        initMustQuery(mustQuery, query);

//         .fetchSource(null, "*")
        Set<String> includeSourceSet = Sets.newHashSet();

        includeSourceSet.add("name");
        includeSourceSet.add("age");
        String[] includeSourceArr = new String[includeSourceSet.size()];
        includeSourceSet.toArray(includeSourceArr);

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setPostFilter(mustQuery)
                .setScroll(new TimeValue(60000)) //为了使用scroll，初始搜索请求应该在查询中指定scroll参数，告诉ElasticSearch需要保持搜索的上下文环境多长时间
                .setSize(100);


        EsUtil.initFetchSource(searchRequestBuilder,includeSourceSet,null);
        Map<String, SortOrder> sortOrderMap = new HashMap<>();
        sortOrderMap.put("age", SortOrder.DESC);

        initSortOrder(searchRequestBuilder, sortOrderMap);

        SearchResponse response = searchRequestBuilder.get();
        System.out.println("---" + response);
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                String json = hit.getSourceAsString();
                System.out.println(json);
                response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().get();
            }
        }
        while (response.getHits().getHits().length != 0);


        /*
            虽然滚动时间已过，搜索上下文会自动被清除，但是一直保持滚动代价会很大，所以当我们不在使用滚动时要尽快使用Clear-Scroll API进行清除。
         */
        ClearScrollRequestBuilder clearBuilder = client.prepareClearScroll();
        clearBuilder.addScrollId(response.getScrollId());
        ClearScrollResponse scrollResponse = clearBuilder.get();
        System.out.println("clearOK ? ：" + scrollResponse.isSucceeded());

    }



    @Test
    public void testGetObjectIdByFilter() throws ExecutionException, InterruptedException {
        Map<String, Object> query = new HashMap<String, Object>();
        query.put("message", "AA testCreateBulkProcessor message");
        query.put("date", "2019-10-03");

        List<String> ids = EsUtil.getIdByMustQuery(client, INDEX, TYPE, query);
        for (String id :
                ids) {
            System.out.println(id);
        }

    }


    /**
     * MultiSearch API 允许在同一个API中执行多个搜索请求。它的端点是 _msearch
     */
    @Test
    public void testMultiSearchApi() {
        SearchRequestBuilder srb1 = client.prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);
        SearchRequestBuilder srb2 = client.prepareSearch().setQuery(QueryBuilders.matchQuery("user", "hhh")).setSize(1);
        MultiSearchResponse multiSearchResponse = client.prepareMultiSearch().add(srb1).add(srb2).get();
        long nbHits = 0;
        for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
            SearchResponse response = item.getResponse();
            nbHits += response.getHits().getTotalHits();
        }
        System.out.println(nbHits);
    }

    @Test
    public void testByRange() {
        QueryBuilder qb = QueryBuilders.rangeQuery("age").gte(10).includeLower(false).lte(13).includeUpper(false);
        SearchResponse getResponse = client.prepareSearch().setQuery(qb).get();
        System.out.println(getResponse);
    }

    @Test
    public void testMatchAllQuery() throws Exception{
        SimpleQueryStringBuilder simpleQueryStringBuilder = new SimpleQueryStringBuilder("C");

        BoolQueryBuilder shouldQuery = new BoolQueryBuilder();

        initShouldQuery(shouldQuery,ImmutableMap.of("query_string","C"));

        int maxSize = 3;
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setQuery(simpleQueryStringBuilder)
                .setScroll(new TimeValue(60000)) //为了使用scroll，初始搜索请求应该在查询中指定scroll参数，告诉ElasticSearch需要保持搜索的上下文环境多长时间
                .setSize(maxSize);
        SearchResponse response = searchRequestBuilder.get();
        System.out.println(response);
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                String json = hit.getSourceAsString();
                System.out.println(json);
//                JSONObject object = JSON.parseObject(json);
//                Set<String> strings = object.keySet();
//                for (String str : strings) {
//                    System.out.println(object.get(str));
//                }
                System.out.println(hit.getSortValues()[0]);
            }
            response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().get();
        } while (response.getHits().getHits().length != 0 && response.getHits().getHits().length < maxSize);


        /*
            虽然滚动时间已过，搜索上下文会自动被清除，但是一直保持滚动代价会很大，所以当我们不在使用滚动时要尽快使用Clear-Scroll API进行清除。
         */
        ClearScrollRequestBuilder clearBuilder = client.prepareClearScroll();
        clearBuilder.addScrollId(response.getScrollId());
        ClearScrollResponse scrollResponse = clearBuilder.get();
        System.out.println("clearOK ? ：" + scrollResponse.isSucceeded());

    }

}

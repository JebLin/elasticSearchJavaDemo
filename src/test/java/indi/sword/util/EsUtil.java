package indi.sword.util;

import com.google.common.collect.Lists;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author jeb_lin
 * 上午11:58 2019/5/27
 */
public class EsUtil {
    public static void initSortOrder(SearchRequestBuilder searchRequestBuilder, Map<String, SortOrder> sortOrderMap) {
        for (Map.Entry<String, SortOrder> entry :
                sortOrderMap.entrySet()) {
            searchRequestBuilder.addSort(entry.getKey(), entry.getValue());
        }

    }


    public static void initRangeQuery(BoolQueryBuilder mustQuery, String name,
                                      long from, long to, boolean includeLower, boolean includeUpper) {
        QueryBuilder qb = QueryBuilders.rangeQuery(name).gte(from).lte(to)
                .includeLower(includeLower).includeUpper(includeUpper);
        mustQuery.must(qb);
    }

    public static void initMustQuery(BoolQueryBuilder mustQuery, Map<String, Object> query) {

        for (Map.Entry<String, Object> entry :
                query.entrySet()) {
            MatchQueryBuilder qb = QueryBuilders.matchQuery(entry.getKey(), entry.getValue());
            mustQuery.must(qb);
        }
    }

    public static void initShouldQuery(BoolQueryBuilder shouldQuery, Map<String, Object> query) {
        for (Map.Entry<String, Object> entry :
                query.entrySet()) {
            MatchQueryBuilder qb = QueryBuilders.matchQuery(entry.getKey(), entry.getValue());
            shouldQuery.should(qb);
        }
    }

    private static List<String> getIdByQuery(TransportClient client, String index, String type,BoolQueryBuilder boolQueryBuilder) {

        List<String> ids = new LinkedList<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(null, "*");

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
                .setTypes(type)
                .setSource(searchSourceBuilder)
                .setPostFilter(boolQueryBuilder)
                .setScroll(new TimeValue(60000)) //为了使用scroll，初始搜索请求应该在查询中指定scroll参数，告诉ElasticSearch需要保持搜索的上下文环境多长时间
                .setSize(100);

        SearchResponse response = searchRequestBuilder.get();
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                ids.add(hit.getId());
                try {
                    response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        while (response.getHits().getHits().length != 0);

        /*
            虽然滚动时间已过，搜索上下文会自动被清除，但是一直保持滚动代价会很大，所以当我们不在使用滚动时要尽快使用Clear-Scroll API进行清除。
         */
        ClearScrollRequestBuilder clearBuilder = client.prepareClearScroll();
        clearBuilder.addScrollId(response.getScrollId());
        ClearScrollResponse scrollResponse = clearBuilder.get();
        System.out.println("getIdByMustQuery roll clearOK ? ：" + scrollResponse.isSucceeded());


        return ids;
    }

    public static List<String> getIdByShouldQuery(TransportClient client, String index, String type, Map<String, Object> query) {
        BoolQueryBuilder shouldQuery = QueryBuilders.boolQuery();
        initShouldQuery(shouldQuery, query);

        return getIdByQuery(client,index,type,shouldQuery);
    }


    public static List<String> getIdByMustQuery(TransportClient client, String index, String type, Map<String, Object> query) {
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        initMustQuery(mustQuery, query);

        return getIdByQuery(client, index, type, mustQuery);
    }

    public static long updateByMustQuery(TransportClient client, String index, String type, Map<String, Object> query,
                                         Map<String,Object> updateMap,boolean batch){
        List<String> ids = getIdByMustQuery(client, index, type, query);
        if(!batch){
            ids = Lists.newArrayList(ids.get(0));
        }

        long updateCount = 0;
        for (String id :
                ids) {
            try {
                UpdateRequest updateRequest = new UpdateRequest();
                updateRequest.index(index);
                updateRequest.type(type);
                updateRequest.id(id);

                updateRequest.doc(EsUtil.getXContentBuilderByMap(updateMap));
                UpdateResponse updateResponse = client.update(updateRequest).get();
                System.out.println(updateResponse);
                System.out.println(updateResponse.getResult());
                if(updateResponse != null && updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)){
                    updateCount++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return updateCount;
    }

    public static XContentBuilder getXContentBuilderByMap(Map<String,Object> map){
        try {
            XContentBuilder builder =  jsonBuilder().startObject();
            for (Map.Entry<String, Object> entry :
                    map.entrySet()) {
                builder.field(entry.getKey(),entry.getValue());
            }
            return builder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void initFetchSource(SearchRequestBuilder searchRequestBuilder,
                                       Set<String> includeSourceSet,
                                       Set<String> excludeSourceSet) {

        if(null == includeSourceSet || null == excludeSourceSet
                || includeSourceSet.size() == 0 || excludeSourceSet.size() == 0){
            return;
        }
        String[] includeSourceArr = new String[0];
        if(null != includeSourceSet){
            includeSourceArr = new String[includeSourceSet.size()];
            includeSourceSet.toArray(includeSourceArr);
        }
        String[] excludeSourceArr = new String[0];
        if(null != excludeSourceSet){
            excludeSourceArr = new String[excludeSourceSet.size()];
            excludeSourceSet.toArray(excludeSourceArr);
        }
        searchRequestBuilder.setFetchSource(includeSourceArr,excludeSourceArr);
    }

}

package indi.sword.operation.read;

import com.google.common.collect.Maps;
import indi.sword.operation.update.TestUserBase;
import org.elasticsearch.action.search.ClearScrollRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import java.util.Map;

import static indi.sword.util.EsUtil.*;

/**
 * @author jeb_lin
 * 上午11:07 2019/6/4
 */
public class TestUser extends TestUserBase {
    /**
     * 2、 distance query  查询
     */
    @Test
    public void testDistanceQuery() throws Exception {

        Map<String, Object> query = Maps.newHashMap();
        query.put("gender", "M");
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        initMustQuery(mustQuery, query);

        GeoDistanceQueryBuilder queryBuilder = QueryBuilders.geoDistanceQuery("location")
                .point(27.506030450791695, 120.39225947435868)
                .distance(100, DistanceUnit.KILOMETERS)
                .geoDistance(GeoDistance.PLANE);


        GeoDistanceSortBuilder sort = new GeoDistanceSortBuilder("location", 27.506030450791695, 120.39225947435868);
        sort.unit(DistanceUnit.KILOMETERS);
        sort.order(SortOrder.ASC);
        sort.geoDistance(GeoDistance.PLANE);

        mustQuery.must(queryBuilder);

        initRangeQuery(mustQuery,"createTime",123,Integer.MAX_VALUE,true,true);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setPostFilter(mustQuery)
                .addSort(sort)
                .setScroll(new TimeValue(60000)) //为了使用scroll，初始搜索请求应该在查询中指定scroll参数，告诉ElasticSearch需要保持搜索的上下文环境多长时间
                .setSize(100);


        SearchResponse response = searchRequestBuilder.get();
        System.out.println(response);
        System.out.println("======");
        System.out.println("======");
        do {
            for (SearchHit hit : response.getHits().getHits()) {
                String json = hit.getSourceAsString();
                System.out.println(json);
                System.out.println("value -> " + hit.getSortValues()[0]);
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
}

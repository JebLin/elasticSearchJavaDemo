package indi.sword.operation.read;

import com.google.common.collect.Maps;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static indi.sword.util.EsUtil.initMustQuery;

/**
 * @author jeb_lin
 * 上午11:56 2019/5/27
 */
public class TestGeo {
    protected TransportClient client = null;

    public static final String INDEX = "test03";

    public static final String TYPE = "my_type001";

    @Before
    public void beforeClient() throws UnknownHostException {
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
    }

    /**
     * 1、使用 BoundingBoxQuery进行查询
     * 左上右下包围的区域查询
     */
    @Test
    public void testGeoBoundingBoxQuery() {
        GeoBoundingBoxQueryBuilder queryBuilder = QueryBuilders.geoBoundingBoxQuery("location")
                .setCorners(new GeoPoint(42.01, -72.1), new GeoPoint(40.01, -71.01));
        SearchResponse searchResponse = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder).get();
        System.out.println(searchResponse);
    }

    /**
     * 2、 distance query  查询
     */
    @Test
    public void testDistanceQuery() throws Exception {

        Map<String, Object> query = Maps.newHashMap();
        query.put("name", "A B");
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        initMustQuery(mustQuery, query);

        GeoDistanceQueryBuilder queryBuilder = QueryBuilders.geoDistanceQuery("location")
                .point(41, -71)
                .distance(200, DistanceUnit.KILOMETERS)
                .geoDistance(GeoDistance.PLANE);

//        mustQuery.must(queryBuilder);
        GeoDistanceSortBuilder sort = new GeoDistanceSortBuilder("location", 41, -71);
        sort.unit(DistanceUnit.KILOMETERS);
        sort.order(SortOrder.ASC);
        sort.geoDistance(GeoDistance.PLANE);

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setPostFilter(mustQuery)
//                .addSort(sort)
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
                System.out.println(hit.getScore());
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



    /**
     * 3、 geoPolygonQuery 查询
     * 多边形包围的区域查询
     */
    @Test
    public void testGeoShapeQuery() {

        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(42, -71));
        points.add(new GeoPoint(40, -72));
        points.add(new GeoPoint(40, -70));
        GeoPolygonQueryBuilder queryBuilder = QueryBuilders.geoPolygonQuery("location", points);

        System.out.println(queryBuilder);

        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(TYPE)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder).get();

        System.out.println("---------");
        System.out.println(response);
    }
}

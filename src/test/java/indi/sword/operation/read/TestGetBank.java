package indi.sword.operation.read;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import indi.sword.operation.TestBankBase;
import indi.sword.util.EsUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author jeb_lin
 * 下午10:01 2019/5/28
 */
public class TestGetBank extends TestBankBase {
    /**
     * Get API
     */
    @Test
    public void testGetApi() {
        BoolQueryBuilder shouldQuery = QueryBuilders.boolQuery();
        Map<String, Object> query = new HashMap<>();
        query.put("address", "826 Overbaugh");
//        query.put("address", "Overbaugh");
        EsUtil.initShouldQuery(shouldQuery,query);
        SearchResponse response = client.prepareSearch(INDEX).setTypes(TYPE)
                .setQuery(shouldQuery).get();
        SearchHit[] hits = response.getHits().getHits();
        System.out.println(hits.length);
        for (int i = 0; i < hits.length; i++) {
            String json = hits[i].getSourceAsString();
            System.out.println(json);
            System.out.println("--------");
        }

    }
}

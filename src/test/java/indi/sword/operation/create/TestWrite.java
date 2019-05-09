package indi.sword.operation.create;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import indi.sword.bean.Blog;
import indi.sword.operation.TestBase;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jeb_lin
 * 上午11:15 2019/5/9
 */
public class TestWrite extends TestBase {


    /**
     * 手动方式
     *
     * @throws UnknownHostException
     */
    @Test
    public void JsonDocument() throws UnknownHostException {
        String json = "{" +
                "\"user\":\"A\"," +
                "\"date\":\"2019-05-09\"," +
                "\"message\":\"A Elasticsearch\"" +
                "}";
        IndexResponse indexResponse = client.prepareIndex(INDEX, TYPE).setSource(json, XContentType.JSON).get();
        System.out.println(indexResponse.getResult());
    }

    /**
     * Map方式
     */
    @Test
    public void MapDocument() {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("user", "B");
        json.put("date", "2019-05-09");
        json.put("message", "B Elasticsearch");
        IndexResponse response = client.prepareIndex(INDEX, TYPE).setSource(json).get();
        System.out.println(response.getResult());
    }

    /**
     * 使用JACKSON序列化
     */
    @Test
    public void JACKSONDocument() throws JsonProcessingException {
        Blog blog = new Blog();
        blog.setUser("C");
        blog.setDate("2019-05-09");
        blog.setMessage("C ElasticSearch");

        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = mapper.writeValueAsBytes(blog);
        IndexResponse response = client.prepareIndex(INDEX, TYPE).setSource(bytes, XContentType.JSON).get();
        System.out.println(response.getResult());
    }

    /**
     * 使用XContentBuilder帮助类方式
     */
    @Test
    public void XContentBuilderDocument() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                .field("user", "D")
                .field("date", "2019-05-09")
                .field("message", "D ElasticSearch").endObject();
        IndexResponse response = client.prepareIndex(INDEX, TYPE).setSource(builder).get();
        System.out.println(response.getResult());
    }

}
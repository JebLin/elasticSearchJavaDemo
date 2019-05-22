package indi.sword.operation.create;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import indi.sword.bean.Blog;
import indi.sword.operation.TestBase;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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
        System.out.println(response.getResult() == DocWriteResponse.Result.CREATED);
    }

    /**
     * 使用XContentBuilder帮助类方式
     */
    @Test
    public void XContentBuilderDocument() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("user", "D")
                .field("date", "2019-05-09")
                .field("message", "D ElasticSearch").endObject();
        IndexResponse response = client.prepareIndex(INDEX, TYPE).setSource(builder).get();
        System.out.println(response.getResult());
    }



    /**
     * 批量插入
     */
    @Test
    public void testBulkApi() throws IOException {
        BulkRequestBuilder requestBuilder = client.prepareBulk();
        requestBuilder.add(client.prepareIndex(INDEX, TYPE, "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "bulk A")
                        .field("date", "2019-05-09")
                        .field("message", "bulk A message")
                        .endObject()));
        requestBuilder.add(client.prepareIndex(INDEX, TYPE, "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "bulk B")
                        .field("postDate", "2019-05-09")
                        .field("message", "bulk B message")
                        .endObject()));
        BulkResponse bulkResponse = requestBuilder.get();
        if (bulkResponse.hasFailures()) {
            System.out.println("error");
        }
    }


    /**
     * 创建Processor实例
     *
     * BulkProcess默认设计
     * bulkActions 1000
     * bulkSize 5mb
     * 不设置flushInterval
     * concurrentRequests为1，异步执行
     * backoffPolicy重试8次，等待50毫秒
     */
    @Test
    public void testCreateBulkProcessor() throws IOException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            //调用Bulk之前执行，例如可以通过request.numberOfActions()方法知道numberOfActions
            public void beforeBulk(long l, BulkRequest request) {

            }

            //调用Bulk之后执行，例如可以通过response.hasFailures()方法知道是否执行失败
            public void afterBulk(long l, BulkRequest request, BulkResponse response) {

            }

            //调用失败抛出throwable
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

            }
        }).setBulkActions(10000) //每次10000个请求
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)) //拆成5MB一块
                .setFlushInterval(TimeValue.timeValueSeconds(5))//无论请求数量多少，每5秒钟请求一次
                .setConcurrentRequests(1)//设置并发请求的数量。值为0意味着只允许执行一个请求。值为1意味着允许1并发请求
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                //设置自定义重复请求机制，最开始等待100毫秒，之后成倍增加，重试3次，当一次或者多次重复请求失败后因为计算资源不够抛出EsRejectedExecutionException
                // 异常，可以通过BackoffPolicy.noBackoff()方法关闭重试机制
                .build();

        //增加requests
        bulkProcessor.add(new IndexRequest(INDEX, TYPE, "4").source(
                jsonBuilder()
                        .startObject()
                        .field("user", "AA testCreateBulkProcessor")
                        .field("date", "2019-05-09")
                        .field("message", "AA testCreateBulkProcessor message")
                        .endObject()));

        // 删除操作
        bulkProcessor.add(new DeleteRequest(INDEX, TYPE, "2"));
        bulkProcessor.flush();
        //关闭bulkProcessor
        bulkProcessor.close();
        client.admin().indices().prepareRefresh().get();
        client.prepareSearch().get();

    }


}
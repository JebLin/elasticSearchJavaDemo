package indi.sword.operation.delete;

import indi.sword.operation.TestBase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.junit.Test;

/**
 * @author jeb_lin
 * 下午12:07 2019/5/9
 */
public class TestDelete extends TestBase {
    /**
     * Get API
     */
    @Test
    public void testGetApi() {
        /*
            索引、类型、_id
         */
        DeleteResponse deleteResponse = client.prepareDelete(INDEX, TYPE, "hvuommoBqn4bnQeoc5BC").get();
        System.out.println(deleteResponse);
        System.out.println("---");

    }

    /**
     * 通过查询条件删除
     */
    @Test
    public void deleteByQuery() {
        BulkByScrollResponse deleteResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", "D")) //查询条件
                .source(INDEX).get();//索引名
        System.out.println(deleteResponse);
        System.out.println(deleteResponse.getDeleted());//删除文档数量
    }

    /**
     * 回调的方式执行删除 适合大数据量的删除操作
     * 当执行的删除的时间过长时，可以使用异步回调的方式执行删除操作，执行的结果在回调里面获取
     * 当程序停止时，在ElasticSearch的控制台依旧在执行删除操作，异步的执行操作
     * 监听回调方法是execute方法
     */
    @Test
    public void DeleteByQueryAsync() {
        for (int i = 1300; i < 3000; i++) {
            DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                    .filter(QueryBuilders.matchQuery("user", "A " + i))
                    .source(INDEX)
                    .execute(new ActionListener<BulkByScrollResponse>() {
                        public void onResponse(BulkByScrollResponse response) {
                            long deleted = response.getDeleted();
                            System.out.println("删除的文档数量为= " + deleted);
                        }

                        public void onFailure(Exception e) {
                            System.out.println("Failure");
                        }
                    });
        }
    }


}

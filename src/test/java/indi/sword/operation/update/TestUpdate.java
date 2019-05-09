package indi.sword.operation.update;

import indi.sword.operation.TestBase;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.script.Script;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author jeb_lin
 * 下午12:13 2019/5/9
 *
 * 主要有两种方法进行更新操作
 * 创建UpdateRequest，通过client发送
 * 使用prepareUpdate()方法。
 */
public class TestUpdate extends TestBase {
    /**
     * 使用UpdateRequest进行更新
     */
    @Test
    public void testUpdateAPI() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(INDEX);
        updateRequest.type(TYPE);
        updateRequest.id("gfujmmoBqn4bnQeoVZDf");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("user", "BBB")
                .endObject());
        client.update(updateRequest).get();
    }

    /**
     * 使用PrepareUpdate
     *
     * client.prepareUpdate中的setScript方法不同的版本的参数不同，这里直接传入值，也可以直接插入文件存储的脚本，然后直接执行脚本里面的数据进行更新操作。
     */
    @Test
    public void testUpdatePrepareUpdateByScript() {
        client.prepareUpdate(INDEX, TYPE, "gfujmmoBqn4bnQeoVZDf")
                .setScript(new Script("ctx._source.user = \"BBB testUpdatePrepareUpdateByScript\"")).get();
    }

    @Test
    public void testUpdatePrepareUpdateByDoc() throws IOException {
        client.prepareUpdate(INDEX, TYPE, "gfujmmoBqn4bnQeoVZDf")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("user", "BBB testUpdatePrepareUpdateByDoc")
                        .endObject()).get();
    }

    /**
     * 通过脚本更新
     */
    @Test
    public void testUpdateByScript() throws ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, "gfujmmoBqn4bnQeoVZDf")
                .script(new Script("ctx._source.user = \"BBB testUpdateByScript\""));
        client.update(updateRequest).get();
    }


    /**
     * 更新文档 如果存在更新，否则插入
     *
     * 如果参数中的_id存在，即index/type/_id存在，那么就会执行UpdateRequest，如果index/type/_id不存在，那么就直接插入
     */
    @Test
    public void testUpsert() throws IOException, ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, "gfujmmoBqn4bnQeoVZDf")
                .source(jsonBuilder()
                        .startObject()
                        .field("user", "BBB testUpsert")
                        .field("date", "2019-05-09")
                        .field("message", "B ElasticSearch")
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest(INDEX, TYPE, "gfujmmoBqn4bnQeoVZDf")
                .doc(jsonBuilder()
                        .startObject()
                        .field("user", "BBB testUpsert")
                        .endObject())
                .upsert(indexRequest); //如果不存在，就增加indexRequest
        client.update(updateRequest).get();
    }

}

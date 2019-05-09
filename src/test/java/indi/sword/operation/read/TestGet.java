package indi.sword.operation.read;


import indi.sword.operation.TestBase;
import org.elasticsearch.action.get.GetResponse;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

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

}

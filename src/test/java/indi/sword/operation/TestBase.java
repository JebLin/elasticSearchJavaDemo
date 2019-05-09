package indi.sword.operation;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author jeb_lin
 * 上午11:41 2019/5/9
 */
public class TestBase {

    protected TransportClient client = null;

    public static final String INDEX = "test";

    public static final String TYPE = "_doc";

    @Before
    public void beforeClient() throws UnknownHostException {
        client = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
    }
}

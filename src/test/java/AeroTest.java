import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import org.junit.Test;

/**
 * Created by SungTae on 2016-12-15.
 */

public class AeroTest {

    AerospikeClient client;

    @Test
    public void join(){
        ClientPolicy cPolicy = new ClientPolicy();
        cPolicy.timeout = 500;
        this.client = new AerospikeClient(cPolicy, "10.3.10.106", 3000);
    }


}

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by seong on 16. 12. 17.
 */
public class ConnectAerospike {
    private AerospikeClient client;
    private String sDbName = "test";
    private String sTable = "COOKIE";
    private Policy policy;
    private WritePolicy wPolicy;
    private Key key;
    private Bin bin;
    private ClientPolicy cPolicy;

    public ConnectAerospike(){
        cPolicy = new ClientPolicy();
        wPolicy = new WritePolicy();
        policy = new QueryPolicy();

        cPolicy.timeout = 500;
        this.client = new AerospikeClient(cPolicy, "localhost", 3000);
    }

    public void setAeroData(String oaid, String ty, byte[] raw) {
        String type = "";

        switch (ty){
            case "Home" : type = "U_HOME"; break;
            case "Login" : type = "U_LOGIN"; break;
            case "Join" : type = "U_JOIN"; break;
            case "Item" : type = "U_ITEM"; break;
            case "Cart" : type = "U_CART"; break;
            case "PurchaseComplete" : type = "U_PURCHASE"; break;
        }

        wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
        key= new Key(sDbName, sTable, oaid);
        bin = new Bin(type, raw);

        this.client.put(wPolicy, key, bin);
    }

    public Record getAeroData(String oaid) {
        Key key = new Key(sDbName, sTable, oaid);
        Record record = client.get(policy, key);

        return record;
    }

    public void deleteData(Set oaid){
        List<String> oaidList = new ArrayList<>(oaid);

        for(int i = 0 ; i < oaidList.size() ; i++){
            Key key = new Key(sDbName, sTable, oaidList.get(i));
            client.delete(wPolicy, key);
        }
    }

    public void aeroClose(){
        this.client.close();
    }
}

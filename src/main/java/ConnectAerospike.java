import com.aerospike.client.*;
import com.aerospike.client.policy.*;

import java.util.List;

/**
 * Created by seong on 16. 12. 17.
 */
public class ConnectAerospike {
    private AerospikeClient client;
    String sDbName = "test";
    String sTable = "COOKIE";

    public ConnectAerospike(){
        ClientPolicy cPolicy = new ClientPolicy();
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

        WritePolicy wPolicy = new WritePolicy();
        wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

        Key key= new Key(sDbName, sTable, oaid);

        Bin bin = new Bin(type, raw);

        this.client.put(wPolicy, key, bin);

    }

    public Record getAeroData(String oaid) {

        Policy policy = new QueryPolicy();
        Key key = new Key(sDbName, sTable, oaid);
        System.out.println(key);
        Record record = client.get(policy, key);

        System.out.println(record.bins.get("username"));
        return record;
    }
}

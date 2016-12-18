import com.aerospike.client.Record;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
/**
 * Created by seong on 16. 12. 18.
 */
public class AeroGetData {
    private ConnectAerospike connectAerospike;

    public AeroGetData(ConnectAerospike connectAerospike) {
        this.connectAerospike = connectAerospike;
    }

    public void getAeroData(Set<String> oaid) throws IOException {
        MessagePack msgpack = new MessagePack();
        Record record;
        List<String> oaidList = new ArrayList<>(oaid);
        JSONObject jsonObject = new JSONObject();

        byte[] raw;
        for(int i = 0 ; i < oaid.size() ; i++){
            record = connectAerospike.getAeroData(oaidList.get(i));
            Iterator iterator = record.bins.keySet().iterator();

            while(iterator.hasNext()){
                String type = (String) iterator.next();
                raw = (byte[]) record.bins.get(type);

                List dst1 = msgpack.read(raw, Templates.tList(Templates.TString));
                jsonObject.put(oaidList.get(i), getJsonArray(type, dst1));
                /*
                json -> {oaid, json array}
                U_HOME >> JSONDATA = {type, array}

                U_ITEM >> JSONDATA = {userid, JSONGOODS}
                       >> JSONGOODS = {goodsid, jsoncate}
                       >> jsoncate = {}
                 */
            }
        }
        System.out.println(jsonObject);

        connectAerospike.aeroClose();
    }

    private JSONArray getJsonArray(String type, List dst1) {
        JSONArray jsonArray = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        String EV1 = "U_HOME U_LOGIN U_JOIN";
        String EV2 = "U_ITEM U_CART U_PURCHASE";

        if(EV1.contains(type)){
            String[] userAndTime = dst1.get(0).toString().split(":");
            JSONObject event1 = new JSONObject();
            event1.put(userAndTime[0],userAndTime[1]);
            jsonObject.put(type, event1);
        }
        if(EV2.contains(type)){
            String[] userData = dst1.get(0).toString().split(",");
            String[] goodsData = userData[2].split(":"); // goods category,time

            JSONObject event2 = new JSONObject();
            JSONArray goodsArray = new JSONArray();
            JSONObject categorys = new JSONObject();
            for(int i = 0 ; i < goodsData.length ; i = i+2){
                categorys.put(goodsData[i], goodsData[i+1]);
            }
            goodsArray.add(categorys);

            event2.put(userData[0],goodsArray);
            jsonObject.put(type, event2);
        }

        jsonArray.add(jsonObject);
        return jsonArray;
    }
}

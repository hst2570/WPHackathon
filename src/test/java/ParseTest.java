import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by SungTae on 2016-12-16.
 */
public class ParseTest {

    private Path wp = Paths.get("C:/Users/SungTae/Desktop/wp/sample.txt");
    private Path home = Paths.get("/home/seong/다운로드/wpc_10_10000.txt");
    private AerospikeClient client;

    public void join(){
        ClientPolicy cPolicy = new ClientPolicy();
        cPolicy.timeout = 500;
        this.client = new AerospikeClient(cPolicy, "10.3.10.106", 3000);
    }

    public List getData() throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(String.valueOf(wp)));

        List<String> data = new ArrayList<String>();
        String s;

        while((s = in.readLine()) != null){
            String[] src = s.split("\t");

            for(int i = 0 ; i < src.length ; i++){
                data.add(src[i]);
            }
        }

        return data;
    }

    @Test
    public void parse() throws IOException {
        List<String> data = getData();
        List<String> oaid = new ArrayList<>();
        List<String> time = new ArrayList<>();
        List<String> event = new ArrayList<>();
        List<String> ti = new ArrayList<>();

        String sDbName = "viewer";
        String sTable = "COOKIE";
        String username = "sthwang";

        MessagePack msgpack = new MessagePack();

        String EVENT1 = "Home Login Join";
        String EVENT2 = "Item Cart PurchaseComplete";

        for(int i = 0 ; i+2 < data.size() ; i = i + 3){
            if(data.get(i).equals("")){
                continue;
            }
            byte[] raw = new byte[0];

            String[] dd = getParse(data.get(i+2));
            oaid.add(data.get(i));
            time.add(data.get(i+1));
            ti.add(dd[1].substring(3));
            event.add(dd[0].substring(3)); // 이벤트 타입

            if(EVENT1.contains(dd[0].substring(3))){
                Map<String, String> tmp = new HashMap<>();

                tmp.put(dd[1].substring(3), data.get(i+1));

                raw = msgpack.write(tmp);

//                setAeroData(data.get(i), raw);
//                System.out.println(tmp);
            }

            if(EVENT2.contains(dd[0].substring(3))){
                String[] detail = getDetailData(dd);

                if(detail[2].equals("")) continue;

//                System.out.println("------------------");
//                for(int j = 0 ; j < detail.length ; j++){
//
//                    System.out.println(detail[j]);
//
//                }
//                System.out.println("------------------");
                Map<String, Map> tmp = new HashMap<>();

                Map<String, List> i1List = new HashMap<>();

                List<String> il = new ArrayList<>();

                il.add("tc : " + data.get(i+1));

                if(!detail[3].equals(""))
                    il.add("p1 : " + detail[3].substring(3)); // 상품 가격

                if(!detail[4].equals(""))
                    il.add("q1 : " + detail[4].substring(3)); // 상품 갯수

                if(!detail[5].equals("") && !(detail[5].equals("t1="))){
                    il.add("t1 : " + detail[5].substring(3)); // 상품명
                }


                i1List.put(detail[2].substring(3), il); // 상품 id
                tmp.put(dd[1].substring(3), i1List);

                raw = msgpack.write(tmp);

//                setAeroData(data.get(i), raw);
//                System.out.println(tmp);
//                System.out.println(i1List);
//                System.out.println(il);
            }
// Deserialize directly using a template
//        List<String> dst1 = msgpack.read(raw, Templates.tList(Templates.TString));
//
//        Iterator iterator = dst1.iterator();
//
//        while (iterator.hasNext()) {
//            System.out.println(iterator.next());
//        }
        }
    }

    private void setAeroData(String ty, byte[] raw) {
        join();
        String username = "sthwang";
        String password = "123123";
        String sDbName = "viewer";
        String sTable = "COOKIE";

        Policy policy = new QueryPolicy();
        Key key= new Key(sDbName, sTable, username);
        System.out.println(key);
        Record record = client.get(policy, key);

        System.out.println(record.bins.get("username"));
    }

    private String[] getDetailData(String[] event2Data) {
        String[] tmpDatas = event2Data;
        String tmp;

        for(int i = 2; i < tmpDatas.length; i++){
            if(tmpDatas[i].contains("i1=")){
                tmp = tmpDatas[2];
                tmpDatas[2] = tmpDatas[i];
                tmpDatas[i] = tmp;
                break;
            }

            if(i == tmpDatas.length-1 && !tmpDatas[i].contains("i1=")){
                tmpDatas[2] = "";
            }
        }

        for(int i = 3; i < tmpDatas.length; i++){
            if(tmpDatas[i].contains("p1=")){
                tmp = tmpDatas[3];
                tmpDatas[3] = tmpDatas[i];
                tmpDatas[i] = tmp;

                break;
            }
            if(i == tmpDatas.length-1 && !tmpDatas[i].contains("p1=")){
                tmpDatas[3] = "";
            }
        }

        for(int i = 4; i < tmpDatas.length; i++){
            if(tmpDatas[i].contains("q1=")){
                tmp = tmpDatas[4];
                tmpDatas[4] = tmpDatas[i];
                tmpDatas[i] = tmp;
                break;
            }

            if(i == tmpDatas.length-1 && !tmpDatas[i].contains("q1=")){
                tmpDatas[4] = "";
            }
        }

        for(int i = 5; i < tmpDatas.length; i++){
            if(tmpDatas[i].contains("t1=")){
                tmp = tmpDatas[5];
                tmpDatas[5] = tmpDatas[i];
                tmpDatas[i] = tmp;
                break;
            }
            if(i == tmpDatas.length-1 && !tmpDatas[i].contains("t1=")){
                tmpDatas[5] = "";
            }
        }

        return tmpDatas;
    }

    private String[] getParse(String value) {
        String[] valueData = value.split("&");
        String tmp;

        for(int i = 0; i < valueData.length; i++){
            if(valueData[i].contains("ty")){
                tmp = valueData[0];
                valueData[0] = valueData[i];
                valueData[i] = tmp;
                break;
            }
        }

        for(int i = 1; i < valueData.length; i++){
            if(valueData[i].contains("ti")){
                tmp = valueData[1];
                valueData[1] = valueData[i];
                valueData[i] = tmp;
                break;
            }
        }

        return valueData;
    }
}

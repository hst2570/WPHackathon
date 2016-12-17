import org.msgpack.MessagePack;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by seong on 16. 12. 17.
 */
public class AeroParse {
    private List<String> data;
    private ConnectAerospike connectAerospike;
    private GetEventData getEventData;

    public AeroParse(){
        connectAerospike = new ConnectAerospike();
        getEventData = new GetEventData();
    }

    public void setAeroData() throws IOException {
        MessagePack msgpack = new MessagePack();
        String EVENT_TYPE1 = "Home Login Join";
        String EVENT_TYPE2 = "Item Cart PurchaseComplete";

        for(int i = 0 ; i+2 < data.size() ; i = i + 3){
            if(data.get(i).equals("")){
                continue;
            }

            byte[] raw;
            String[] dd = getEventData.getParse(data.get(i+2));

            if(EVENT_TYPE1.contains(dd[0].substring(3))){
                List<String> setEvent = new ArrayList<>();
                setEvent.add(dd[1].substring(3) + ":" + data.get(i+1));
                raw = msgpack.write(setEvent);

                connectAerospike.setAeroData(data.get(i), data.get(i), raw);
            }

            if(EVENT_TYPE2.contains(dd[0].substring(3))){
                String[] detail = getEventData.getDetailData(dd);

                if(detail[2].equals("")) continue;

                List<Map> setEvent2 = new ArrayList<>();
                Map<String, Map> tmp = new HashMap<>();
                Map<String, List> i1List = new HashMap<>();
                List<String> il = new ArrayList<>();

                il.add("tc:" + data.get(i+1));
                if(!detail[3].equals(""))
                    il.add("p1:" + detail[3].substring(3)); // 상품 가격
                if(!detail[4].equals(""))
                    il.add("q1:" + detail[4].substring(3)); // 상품 갯수
                if(!detail[5].equals("") && !(detail[5].equals("t1="))){
                    il.add("t1:" + detail[5].substring(3)); // 상품명
                }

                i1List.put(detail[2].substring(3), il); // 상품 id별 카테고리와 데이터
                tmp.put(dd[1].substring(3), i1List); // 사용자
                setEvent2.add(tmp);

                raw = msgpack.write(setEvent2);
                connectAerospike.setAeroData(data.get(i), data.get(i), raw);
            }
        }
    }

    public void setData(String[] args) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(String.valueOf(args)));

        List<String> data = new ArrayList<String>();
        String s;

        while((s = in.readLine()) != null){
            String[] src = s.split("\t");

            for(int i = 0 ; i < src.length ; i++){
                data.add(src[i]);
            }
        }
        this.data = data;
    }
}

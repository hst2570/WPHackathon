import com.aerospike.client.Record;
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
    private Path home = Paths.get("/home/seong/Downloads/wpc_100_10000.txt");
    private ConnectAerospike connectAerospike;
    private GetEventData getEventData;

    Set<String> oaid = new HashSet<>();

    public ParseTest(){
        getEventData = new GetEventData();
        connectAerospike = new ConnectAerospike();
    }

    public List getData() throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(String.valueOf(home)));

        List<String> data = new ArrayList<>();
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
        String setType;
        String user;

        MessagePack msgpack = new MessagePack();
        String EVENT_TYPE1 = "Home Login Join";
        String EVENT_TYPE2 = "Item Cart PurchaseComplete";


        for(int i = 0 ; i+2 < data.size() ; i = i + 3){
            if(data.get(i).equals("")){
                continue;
            }

            String[] homeData = getEventData.getParse(data.get(i+2));
            setType = homeData[0].substring(3);
            user = homeData[1].substring(3);
            byte[] raw;

            if(EVENT_TYPE1.contains(setType)){
                List<String> setEvent = new ArrayList<>();
                setEvent.add(user + ":" + data.get(i+1));
                oaid.add(data.get(i));

                raw = msgpack.write(setEvent);

                connectAerospike.setAeroData(data.get(i), homeData[0].substring(3), raw);
            }

            if(EVENT_TYPE2.contains(setType)){
                String[] detail = getEventData.getDetailData(homeData);

                if(detail[2].equals("")) continue;
                oaid.add(data.get(i));

                List<String> setEvent2 = new ArrayList<>();
                String bottom = user + ":{" + detail[2].substring(3) + ":{" +"tc:" + data.get(i+1);

                if(!detail[3].equals("")) {
                    bottom = bottom + ",p1:"+detail[3].substring(3); // 상품 가격
                }
                if(!detail[4].equals("")) {
                    bottom = bottom + ",q1:"+detail[4].substring(3);
                }
                if(!detail[5].equals("") && !(detail[5].equals("t1="))){
                    bottom = bottom + ",t1:"+detail[5].substring(3);
                }
                bottom = bottom + "}}";

                setEvent2.add(bottom);

                raw = msgpack.write(setEvent2);
                connectAerospike.setAeroData(data.get(i), homeData[0].substring(3), raw);
            }

        }

        getAeroData(oaid);

        connectAerospike.deleteData(oaid);
        connectAerospike.aeroClose();
    }

    private boolean checkOaid(String s) {
        System.out.println(s);
        return oaid.contains(s);
    }


    //    @Test
    public void getAeroData(Set<String> oaid) throws IOException {
        MessagePack msgpack = new MessagePack();
        Record record;
        List<String> oaidList = new ArrayList<>(oaid);
        for(int i = 0 ; i < oaid.size() ; i++){
            record = connectAerospike.getAeroData(oaidList.get(i));
            if(record.bins.containsKey("U_ITEM")) {
                byte[] raw = (byte[]) record.bins.get("U_ITEM");

                if (raw != null) {
                    List<String> dst1 = msgpack.read(raw, Templates.tList(Templates.TString));

                    Iterator iterator = dst1.iterator();

                    while (iterator.hasNext()) {
                        System.out.println("U_ITEM:{"+iterator.next()+"}");
                    }
                }
            }


        }

    }

    public String setType(String ty){
        String type = "";

        return type;
    }

    public void deleteData(List<String> oaid){

    }
}

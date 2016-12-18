import org.msgpack.MessagePack;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by seong on 16. 12. 17.
 */
public class AeroParse {
    private List<String> data;
    private ConnectAerospike connectAerospike;
    private GetEventData getEventData;
    private Set<String> oaid = new HashSet<>();

    public AeroParse(ConnectAerospike connectAerospike){
        this.connectAerospike = connectAerospike;
        getEventData = new GetEventData();
    }

    public void setAeroData() throws IOException {
        String setType;
        String user;

        MessagePack msgpack = new MessagePack();
        String EVENT_TYPE1 = "Home Login Join";
        String EVENT_TYPE2 = "Item Cart PurchaseComplete";

        System.out.println("Set Data");

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

//        connectAerospike.aeroClose();
        System.out.println("-- Done --");
    }

    public void setData(Path args) throws IOException {
//        System.out.println(args.toString());
        BufferedReader in = new BufferedReader(new FileReader(String.valueOf(args)));

        List<String> data = new ArrayList<>();
        String s;

        while((s = in.readLine()) != null){
            String[] src = s.split("\t");

            for(int i = 0 ; i < src.length ; i++){
                data.add(src[i]);
            }
        }
        this.data = data;
    }

    public Set<String> getOaid() {
        return this.oaid;
    }
}

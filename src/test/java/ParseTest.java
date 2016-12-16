import models.Model;
import org.junit.Test;
import org.msgpack.MessagePack;

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

    private Path wp = Paths.get("C:/Users/SungTae/Desktop/wp/wpc_10_10000.txt");
    private Path home = Paths.get("/home/seong/다운로드/wpc_10_10000.txt");

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

        MessagePack msgpack = new MessagePack();

        String EVENT1 = "Home Login Join";
        String EVENT2 = "Item Cart PurchaseComplete";

        for(int i = 0 ; i+2 < data.size() ; i = i + 3){
            if(data.get(i).equals("")){
                continue;
            }

            String[] dd = getParse(data.get(i+2));
//            System.out.println();
            oaid.add(data.get(i));
            time.add(data.get(i+1));
            ti.add(dd[1].substring(3));
            event.add(dd[0].substring(3)); // 이벤트 타입

            if(EVENT1.contains(dd[0].substring(3))){

            }

            if(EVENT2.contains(dd[0].substring(3))){
                String[] detail = getDetailData(dd);
                System.out.println(detail[2]);
            }
        }

//        byte[] raw = msgpack.write(parse);
//        String<String, >

//        parse.put(data.get(0), data.get(1));
//        parse.put(data.get(0), data.get(2));


//        for(int i = 0 ; i < data.size() ; i++){
//            if(data.get(i).equals("")){
//                System.out.println(i);
//            }
//        }

//        System.out.println(data.get(85));
//        while (iterator.hasNext()) {
//            System.out.println(iterator.next());
//        }
    }

    private String[] getDetailData(String[] s) {
        String[] dd = s;
        String tmp;

        for(int i = 2; i < dd.length; i++){
            if(dd[i].contains("i1")){
                tmp = dd[2];
                dd[2] = dd[i];
                dd[i] = tmp;
                break;
            }

            if(i == dd.length-1){
                dd[2] = "";
                return dd;
            }
        }

        for(int i = 3; i < dd.length; i++){
            if(dd[i].contains("p1")){
                tmp = dd[3];
                dd[3] = dd[i];
                dd[i] = tmp;
                break;
            }
        }

        for(int i = 4; i < dd.length; i++){
            if(dd[i].contains("q1")){
                tmp = dd[4];
                dd[4] = dd[i];
                dd[i] = tmp;
                break;
            }
        }

        for(int i = 5; i < dd.length; i++){
            if(dd[i].contains("t1")){
                tmp = dd[5];
                dd[5] = dd[i];
                dd[i] = tmp;
                break;
            }
        }

        return dd;
    }

    private String[] getParse(String s) {
        String[] src = s.split("&");
        String tmp;

        for(int i = 0; i < src.length; i++){
            if(src[i].contains("ty")){
                tmp = src[0];
                src[0] = src[i];
                src[i] = tmp;
                break;
            }
        }

        for(int i = 1; i < src.length; i++){
            if(src[i].contains("ti")){
                tmp = src[1];
                src[1] = src[i];
                src[i] = tmp;
                break;
            }
        }

        return src;
    }
}

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.*;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Created by SungTae on 2016-12-15.
 */

public class AeroTest {

    AerospikeClient client;
    Key key;
//    @Test
    public void join(){
        ClientPolicy cPolicy = new ClientPolicy();
        cPolicy.timeout = 500;
        this.client = new AerospikeClient(cPolicy, "10.3.10.106", 3000);
    }

//    @Test
    public void setData() {
        this.join();
        String username = "sthwang";
        String password = "123123";
        String sDbName = "viewer";
        String sTable = "COOKIE";

        if (username != null && username.length() > 0) {

            // Write record
            WritePolicy wPolicy = new WritePolicy();
            wPolicy.recordExistsAction = RecordExistsAction.UPDATE;

            key = new Key(sDbName, sTable, username);
            Bin bin1 = new Bin("username", username);
            Bin bin2 = new Bin("password", password);

            System.out.println(key);
            System.out.println(bin1);
            System.out.println(bin2);

            this.client.put(wPolicy, key, bin1, bin2);
        }
    }

//    @Test
    public void getData(){
        this.join();

        String username = "sthwang";
        String password = "123123";
        String sDbName = "viewer";
        String sTable = "COOKIE";

        Policy policy = new QueryPolicy();
        key = new Key(sDbName, sTable, username);

        Record record = client.get(policy, key);

        System.out.println(record);
    }

//    @Test
    public void streamTest() throws IOException{
        Path path = Paths.get("C:/Users/SungTae/Desktop/wp/wpc_10_10000.txt");
        Stream<String> stream;

        // Files.lines() 메소드 이용
        stream = Files.lines(path, Charset.defaultCharset());
        stream.forEach(System.out::println);
        System.out.println();

        // BufferedReader의 lines() 메소드 이용
        File file = path.toFile();
        FileReader fileReader = new FileReader(file);
        BufferedReader br = new BufferedReader(fileReader);
        stream = br.lines();
        stream.forEach(System.out::println);

        stream.close();
        br.close();
    }

    @Test
    public void readFile(){
        Path path = Paths.get("/home/seong/다운로드/wpc_10_10000.txt");

        try {
            BufferedReader in = new BufferedReader(new FileReader(String.valueOf(path)));
            String s;

            while ((s = in.readLine()) != null) {
                if(s.contains("v1"))
                System.out.println(s);
            }
            in.close();
        } catch (IOException e) {
            System.err.println(e); // 에러가 있다면 메시지 출력
            System.exit(1);
        }
    }
}

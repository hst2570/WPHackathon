import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.*;
import models.Model;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by SungTae on 2016-12-15.
 */

public class AeroTest {

    private AerospikeClient client;
    private Key key;

    private Path wp = Paths.get("C:/Users/SungTae/Desktop/wp/wpc_10_10000.txt");
    private Path home = Paths.get("/home/seong/다운로드/wpc_10_10000.txt");

//    @Test
    public void join(){
        ClientPolicy cPolicy = new ClientPolicy();
        cPolicy.timeout = 500;
        this.client = new AerospikeClient(cPolicy, "localhost", 3000);
    }

    @Test
    public void setData() {
        this.join();
        String username = "sthwang";
        String password = "123123";
        String sDbName = "test";
        String sTable = "COOKIE";

        if (username != null && username.length() > 0) {

            // Write record
            WritePolicy wPolicy = new WritePolicy();
            wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
            List<String> test = new ArrayList<>();
            test.add("asd");
            test.add("asdd");
            test.add("asddd");

            key = new Key(sDbName, sTable, username);
            Bin bin1 = new Bin("username", test);
            Bin bin2 = new Bin("password", password);

//            System.out.println(key);
//            System.out.println(bin1);
//            System.out.println(bin2);

            this.client.put(wPolicy, key, bin1, bin2);
        }
    }

    @Test
    public void getData(){
        this.join();

        String username = "sthwang";
        String password = "123123";
        String sDbName = "test";
        String sTable = "COOKIE";

        Policy policy = new QueryPolicy();
        key = new Key(sDbName, sTable, username);
        System.out.println(key);
        Record record = client.get(policy, key);

        System.out.println(record.bins.get("username"));
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

//    @Test
    public void testMsgPack() throws IOException {
        // Create serialize objects.
        BufferedReader in = new BufferedReader(new FileReader(String.valueOf(wp)));

        Map<String, String> src = new HashMap<>();

        String s;
        while ((s = in.readLine()) != null) {
            src.put(s, s);
        }

        MessagePack msgpack = new MessagePack();
        byte[] raw = msgpack.write(src);
        // Serialize


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

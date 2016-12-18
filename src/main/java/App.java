
/**
 * Created by SungTae on 2016-12-15.
 */

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public class App {

    public static void main(String[] args) throws IOException {
        System.out.println(args.toString());

        ConnectAerospike connectAerospike = new ConnectAerospike();
        AeroParse parse = new AeroParse(connectAerospike);
        AeroGetData aeroGetData = new AeroGetData(connectAerospike);

        Set<String> oaid;

        parse.setData(args); // 물어볼 것 : 아귀먼트를 파이프로 받아서 리드라인 하는 방법
        parse.setAeroData();

        oaid = parse.getOaid();

        aeroGetData.getAeroData(oaid);
    }
}

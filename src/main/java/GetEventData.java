/**
 * Created by seong on 16. 12. 17.
 */
public class GetEventData {

    public String[] getDetailData(String[] event2Data) {
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

    public String[] getParse(String value) {
        String[] valueData = value.split("&");
        String tmp;

        for(int i = 0; i < valueData.length; i++){
            if(valueData[i].contains("ty")){
                tmp = valueData[0];
                valueData[0] = valueData[i];
                valueData[i] = tmp;
            }else if(valueData[i].contains("ti")){
                tmp = valueData[1];
                valueData[1] = valueData[i];
                valueData[i] = tmp;
            }
        }
        return valueData;
    }
}

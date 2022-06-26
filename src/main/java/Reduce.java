import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;

public  class Reduce extends Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
        try{
            JSONObject obj = new JSONObject();
            obj.put("product_id",key.toString());
            Iterator<Text> it=values.iterator();

            while(it.hasNext()){
                String st=it.next().toString();
                if(st.charAt(0)=='0'){
                    obj.put("brand", st.substring(1));
                }else if(st.charAt(0)=='1'){
                    obj.put("brand_name", st.substring(1));
                }else if(st.charAt(0)=='2'){
                    obj.put("category", st.substring(1));
                }else if(st.charAt(0)=='3'){
                    obj.put("category_name", st.substring(1));
                }
            }

            context.write(NullWritable.get(), new Text(obj.toString()));
        }catch(JSONException e){
            e.printStackTrace();
        }

    }
}

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;


public class MapProduct extends Mapper<LongWritable,Text,Text,Text>{
    enum Brands{
        BRAND_CAP, BRAND_NAME_CAP, PRODUCT_CATEGORY_CAP, PRODUCT_CATEGORY_NAME_CAP
    }
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String product_id;
        String label;
        String line=value.toString();
        String [] tuple=line.split("\\n");
        try{
            for(int i=0;i<tuple.length;i++)
            {
                JSONObject obj=new JSONObject(tuple[i]);
                product_id=obj.getString("product_id");
                if(obj.has("brand")){
                    label="0"+obj.getString("brand");
                } else if(obj.has("brand_name")){
                    label="1"+obj.getString("brand_name");
                }else if(obj.has("category")){
                    label="2"+obj.getString("category");
                }
                else {
                    label="3"+obj.getString("category_name");
                }

                if(obj.has("brand") && Character.isUpperCase(obj.getString("brand").charAt(0))){
                    context.getCounter(Brands.BRAND_CAP).increment(1);
                }
                if(obj.has("brand_name") && Character.isUpperCase(obj.getString("brand_name").charAt(0))){
                    context.getCounter(Brands.BRAND_NAME_CAP).increment(1);
                }
                if(obj.has("category") && Character.isUpperCase(obj.getString("category").charAt(0))){
                    context.getCounter(Brands.PRODUCT_CATEGORY_CAP).increment(1);
                }
                if(obj.has("category_name") && Character.isUpperCase(obj.getString("category_name").charAt(0))){
                    context.getCounter(Brands.PRODUCT_CATEGORY_NAME_CAP).increment(1);
                }

                context.write(new Text(product_id), new Text(label));
            }
        }
        catch(JSONException e){
            e.printStackTrace();
        }
    }
}
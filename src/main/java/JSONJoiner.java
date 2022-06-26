import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JSONJoiner extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JSONJoiner(), args);
        System.exit(res);
    }

    @Override
    public int run(String args[])throws Exception {
        Configuration conf= getConf();
        conf.set("mapred.job.queue.name","d_bi");
        Job job = Job.getInstance(conf,"combine_products");
        job.setJarByClass(JSONJoiner.class);

        job.setMapperClass(MapProduct.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        Path outputPath = new Path(args[3]);


        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,MapProduct.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,MapProduct.class);
        MultipleInputs.addInputPath(job,new Path(args[2]),TextInputFormat.class,MapProduct.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));





        return job.waitForCompletion(true) ? 0 : 1;
    }
}
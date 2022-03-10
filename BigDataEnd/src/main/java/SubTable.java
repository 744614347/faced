import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SubTable {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().trim().split(" ",3);
            context.write(new Text(fields[2]), value);
        }
    }

    public static class Reduce extends Reducer<Text,Text, Text,Text> {
        private MultipleOutputs<Text,Text> multipleOutputs;

        protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {

            for (Text value : values) {
                multipleOutputs.write(new Text(""),value,key.toString());
            }
        }
        protected void setup(Context context){
            multipleOutputs = new MultipleOutputs<Text, Text>(context);
        }
        protected void cleanup(Context context) throws IOException, InterruptedException{
            multipleOutputs.close();
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://114.132.227.132:9000");
        String[] otherArgs = new String[]{"input", "output"}; /* 直接设置输入参数 */
        if (otherArgs.length != 2) {
            System.err.println("SubTable <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "SubTable");
        job.setJarByClass(SubTable.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleOutputs.addNamedOutput(job, "2014", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "2015", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "2016", TextOutputFormat.class, Text.class, Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

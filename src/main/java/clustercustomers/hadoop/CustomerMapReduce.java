package clustercustomers.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerMapReduce
{


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
        Job job = new Job(conf, "customer_map_reduce");
        job.setJarByClass(VectorCreationMapReduce.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(AllTranslationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPaths(job, "/chopin_customers,/chopin_customers_rfqs");
        FileOutputFormat.setOutputPath(job, new Path("aggregated_customers"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

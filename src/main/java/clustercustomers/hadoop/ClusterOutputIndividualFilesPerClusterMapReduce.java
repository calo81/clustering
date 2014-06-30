package clustercustomers.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 30/05/2014
 * Time: 13:10
 * To change this template use File | Settings | File Templates.
 */
public class ClusterOutputIndividualFilesPerClusterMapReduce extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new ClusterOutputIndividualFilesPerClusterMapReduce(), args);
        System.exit(res);
    }

    public static class ClusterPassThroughMapper extends Mapper<IntWritable, WeightedVectorWritable, IntWritable, Text> {
        public void map(IntWritable key, WeightedVectorWritable value, Context context) throws IOException, InterruptedException {
          NamedVector vector = (NamedVector) value.getVector();
          context.write(key,new Text(vector.getName()));
        }
    }

    public static class ClusterPointsToIndividualFile extends Reducer<IntWritable, Text, IntWritable, Text> {
        private MultipleOutputs mos;

        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }


        public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for(Text text: value){
                mos.write("seq", key, text, "cluster-"+key.toString());
                context.write(key,text);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        conf.set("fs.default.name", "hdfs://10.10.0.52:9000/");
        conf.set("mapred.job.tracker", "10.10.0.52:9001");
        Job job = new Job(conf, "cluster-to-individual-files");
        job.setJarByClass(ClusterOutputIndividualFilesPerClusterMapReduce.class);
        job.setMapperClass(ClusterPassThroughMapper.class);
        job.setReducerClass(ClusterPointsToIndividualFile.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        MultipleOutputs.addNamedOutput(job, "seq", TextOutputFormat.class,
                IntWritable.class, Text.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPaths(job, "customer-kmeans/clusteredPoints");
        FileOutputFormat.setOutputPath(job, new Path("individual-clusters"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

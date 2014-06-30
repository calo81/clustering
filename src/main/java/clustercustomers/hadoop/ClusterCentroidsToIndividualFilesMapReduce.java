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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 30/05/2014
 * Time: 13:10
 * To change this template use File | Settings | File Templates.
 */
public class ClusterCentroidsToIndividualFilesMapReduce extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new ClusterCentroidsToIndividualFilesMapReduce(), args);
        System.exit(res);
    }

    public static class ClusterPassThroughMapper extends Mapper<IntWritable, ClusterWritable, IntWritable, VectorWritable> {
        public void map(IntWritable key, ClusterWritable value, Context context) throws IOException, InterruptedException {
            Vector vector = value.getValue().getCenter();
            context.write(key,new VectorWritable(vector));
        }
    }

    public static class ClusterCentroidsToIndividualFile extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {
        private MultipleOutputs mos;

        public void setup(Context context) {
            mos = new MultipleOutputs(context);
        }

        public void reduce(IntWritable key, Iterable<VectorWritable> value, Context context) throws IOException, InterruptedException {
            for(VectorWritable vector: value){
                mos.write("seq", key, vector, "centroid-cluster-"+key.toString());
                context.write(key,vector);
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
        Job job = new Job(conf, "cluster-centroids-to-individual-files");
        job.setJarByClass(ClusterCentroidsToIndividualFilesMapReduce.class);
        job.setMapperClass(ClusterPassThroughMapper.class);
        job.setReducerClass(ClusterCentroidsToIndividualFile.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(VectorWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class,
                IntWritable.class, VectorWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPaths(job, "customer-kmeans/clusters-2-final");
        FileOutputFormat.setOutputPath(job, new Path("individual-centroids"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

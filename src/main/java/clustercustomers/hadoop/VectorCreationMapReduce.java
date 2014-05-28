package clustercustomers.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class VectorCreationMapReduce extends Configured implements Tool {

    public static class VectorizerMapper extends Mapper<LongWritable, Text, Text, VectorWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("DHAJAGSJDHGAJSHGD"+value.toString());
            String[] values = value.toString().split(",");

            VectorWritable writer = new VectorWritable();
            NamedVector vector = new NamedVector(new DenseVector(new double[]{0, 10, 4}), values[0]);
            writer.set(vector);
            context.write(new Text(values[0]), writer);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new VectorCreationMapReduce(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");
        Job job = new Job(conf, "customer_to_vector_mapreduce");
        job.setJarByClass(VectorCreationMapReduce.class);
        job.setMapperClass(VectorizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VectorWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPaths(job, "aggregated_customers");
        FileOutputFormat.setOutputPath(job, new Path("vector_seq_file"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

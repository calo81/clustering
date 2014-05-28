package clustercustomers.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.StringTokenizer;

public class VectorCreationMapReduce
{

    public static class VectorizerMapper extends Mapper<Text, Text, Writable, Writable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
            String[] values = value.toString().split(",");

            VectorWritable writer = new VectorWritable();
            NamedVector vector = new NamedVector(new DenseVector(new double[]{0, 10, 4}), values[0]);
            writer.set(vector);
            context.write(NullWritable.get(), writer);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "|");
        Job job = new Job(conf, "customer_to_vector_mapreduce");
        job.setJarByClass(VectorCreationMapReduce.class);
        job.setMapperClass(VectorizerMapper.class);
        job.setOutputKeyClass(Writable.class);
        job.setOutputValueClass(Writable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPaths(job, "/aggregated_customers");
        FileOutputFormat.setOutputPath(job, new Path("vector_seq_file"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

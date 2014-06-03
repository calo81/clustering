package clustercustomers.hadoop;

import clustercustomers.sqoop.QueryResult;
import org.apache.commons.lang3.ArrayUtils;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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

/**
 * Order in which the data arrives here:
 * <p/>
 * (customer_id,vertical,trade,annual_turnover, claim_count)
 * <p/>
 * Verticals. let's put three features
 */
public class VectorCreationMapReduce extends Configured implements Tool {

    public static class VectorizerMapper extends Mapper<LongWritable, QueryResult, Text, VectorWritable> {

        public void map(LongWritable key, QueryResult value, Context context) throws IOException, InterruptedException {

            VectorWritable writer = new VectorWritable();
            double[] verticals = vectorForVertical(value.get_product());
            double[] trade = vectorForTrade(value.get_primary_trade());
            double[] turnover = vectorForDouble(value.get_annual_turnover());
            double[] claimCount = vectorForDouble(value.get_claim_count());
            NamedVector vector = new NamedVector(new DenseVector(concatArrays(verticals, trade, turnover, claimCount)), value.get___id());
            writer.set(vector);
            context.write(new Text(value.get___id()), writer);
        }

        private double[] vectorForDouble(String valueString) {
            double value = 0;
            if (valueString != null) {
                value = Double.parseDouble(valueString);
            }
            return new double[]{value};
        }

        private double[] vectorForTrade(String value) {
            return new double[]{is(value, "Accountant"), is(value, "Carpenter")};  //To change body of created methods use File | Settings | File Templates.
        }

        private double[] vectorForVertical(String value) {
            return new double[]{is(value, "business"), is(value, "landlord"), is(value, "shop")};  //To change body of created methods use File | Settings | File Templates.
        }

        private int is(String value, String expected) {
            return (value != null && value.equals(expected)) ? 1 : 0;
        }

        private double[] concatArrays(double[]... arrays) {
            double[] aggregatedArrays = new double[]{};
            for (double[] array : arrays) {
                aggregatedArrays = ArrayUtils.addAll(aggregatedArrays, array);
            }
            return aggregatedArrays;
        }
    }

    public static void main(String[] args) throws Exception {
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
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPaths(job, "aggregated_customers");
        FileOutputFormat.setOutputPath(job, new Path("vector_seq_file"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

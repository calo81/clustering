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

    public static class VectorizerMapper extends Mapper<LongWritable, Text, Text, VectorWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            VectorWritable writer = new VectorWritable();
            System.out.println(value.toString());
            String[] values = value.toString().split("\\|");
            double[] verticals = vectorForVertical(values[1]);
            double[] trade = vectorForTrade(values[2]);
            double[] turnover = vectorForDouble(values[3]);
            double[] claimCount = vectorForDouble(values[4]);
            double[] xCoordinate = vectorForDouble(values[7]);
            double[] yCoordinate = vectorForDouble(values[7]);
            NamedVector vector = new NamedVector(new DenseVector(concatArrays(verticals, trade, turnover, claimCount, xCoordinate, yCoordinate)), values[0]);
            writer.set(vector);
            context.write(new Text(values[0]), writer);
        }

        private double[] vectorForDouble(String valueString) {
            double value = 0;
            if (valueString != null && !valueString.isEmpty()) {
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
        conf.set("fs.default.name", "hdfs://10.10.0.52:9000/");
        conf.set("mapred.job.tracker", "10.10.0.52:9001");
        Job job = new Job(conf, "customer_to_vector_mapreduce");
        job.setJarByClass(VectorCreationMapReduce.class);
        job.setMapperClass(VectorizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VectorWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPaths(job, "aggregated_customers_with_coordinates");
        FileOutputFormat.setOutputPath(job, new Path("vector_seq_file"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

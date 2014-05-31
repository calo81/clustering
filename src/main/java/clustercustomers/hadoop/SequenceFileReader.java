package clustercustomers.hadoop;

import clustercustomers.sqoop.QueryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.VectorWritable;


public class SequenceFileReader {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");

        FileSystem fs = FileSystem.get(conf);


        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                new Path("/user/cscarion/aggregated_customers/part-m-00001"), conf);
        LongWritable key = new LongWritable();
        QueryResult value = new QueryResult();


        while (reader.next(key, value)) {
            System.out.println(key.toString() + "  " + value.toString());
            //System.out.println(value.getValue().getCenter());
        }
        reader.close();

    }
}

package clustercustomers.hadoop;

import clustercustomers.sqoop.QueryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;


public class SequenceFileReader {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");

        FileSystem fs = FileSystem.get(conf);


        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                new Path("/user/cscarion/vector_seq_file/part-m-00000"), conf);
        Text key = new Text();
        VectorWritable value = new VectorWritable();


        while (reader.next(key, value)) {
            System.out.println(key.toString() + " " + value.toString());
        }
        reader.close();

    }
}

package clustercustomers.hadoop.fakes;

import clustercustomers.sqoop.QueryResult;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 03/06/2014
 * Time: 10:28
 * To change this template use File | Settings | File Templates.
 */
public class AddingFakeQuestionsToUsersMapReduce extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new AddingFakeQuestionsToUsersMapReduce(), args);
        System.exit(res);
    }

    public static class AddFakeQuestionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private List<Map<String, String>> questions = QuestionsAnswers.QUESTIONS_ANSWERS;
        private Random rand = new Random();

        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String newValue = value.toString()+"|"+questions.get(rand.nextInt(3)).keySet().iterator().next();
            System.out.println(newValue);
            context.write(NullWritable.get(),new Text(newValue));
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = super.getConf();
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");
        Job job = new Job(conf, "add_fake_questions");
        job.setJarByClass(AddingFakeQuestionsToUsersMapReduce.class);
        job.setMapperClass(AddFakeQuestionMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPaths(job, "aggregated_customers_text");
        FileOutputFormat.setOutputPath(job, new Path("aggregated_customers_text_questions"));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

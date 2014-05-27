package clustercustomers.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 27/05/2014
 * Time: 17:42
 * To change this template use File | Settings | File Templates.
 */
public class WordMapper extends Mapper<Text, Text, Text, Text> {

    private Text word = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        System.out.println(value.toString());
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(key, word);
        }
    }
}

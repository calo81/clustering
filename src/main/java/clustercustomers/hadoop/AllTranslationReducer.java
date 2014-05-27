package clustercustomers.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 27/05/2014
 * Time: 17:54
 * To change this template use File | Settings | File Templates.
 */
public class AllTranslationReducer extends Reducer<Text,Text,Writable,Text>
{
    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException
    {
        Map<String, String> translations = new HashMap<String, String>();
        for (Text val : values)
        {
            Logger.getAnonymousLogger().info(val.toString());
            String[] keyAndValue = val.toString().split(":");
            translations.put(keyAndValue[0],keyAndValue[1]);
        }

        StringBuilder finalElement = new StringBuilder();
        finalElement.append(key.toString().split(":")[1]).append(",");
        finalElement.append(translations.get("email_address")).append(",");
        finalElement.append(translations.get("first_name")).append(",");
        finalElement.append(translations.get("vertical"));
        result.set(finalElement.toString());
        context.write(NullWritable.get(), result);
    }
}

package clustercustomers.camel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 27/05/2014
 * Time: 14:21
 * To change this template use File | Settings | File Templates.
 */
public class CustomerMongoPersonalToSequenceFileProcessor implements Processor{
    @Override
    public void process(Exchange exchange) throws Exception {
        String custom = exchange.getIn().getBody(String.class);
        BasicDBList dbList =  (BasicDBList)JSON.parse(custom);
        StringBuilder csv = new StringBuilder();
        for(Object documentObject: dbList){
          BasicDBObject document = (BasicDBObject) documentObject;
          csv.append("id:"+document.get("_id")+"|");
            CsvAppender.appendToCSV(csv, document, "first_name");
            CsvAppender.appendToCSV(csv, document, "email_address");
            CsvAppender.appendToCSV(csv, document, "last_name");
            CsvAppender.appendToCSV(csv, document, "uk_postcode");
            csv.deleteCharAt(csv.lastIndexOf(",")).append("\n");
        }
        exchange.getIn().setBody(csv.toString());
    }
}

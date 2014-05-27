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
        String csv = "";
        for(Object documentObject: dbList){
          BasicDBObject document = (BasicDBObject) documentObject;
          csv += "id:"+document.get("_id")+"|"+"first_name:"+document.get("first_name")+","+"email_address:"+document.get("email_address")+"\n";
        }
        exchange.getIn().setBody(csv);
    }
}

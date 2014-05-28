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
 * Time: 16:36
 * To change this template use File | Settings | File Templates.
 */
public class CustomerMongoWebRfqToSequenceFile implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        String custom = exchange.getIn().getBody(String.class);
        BasicDBList dbList =  (BasicDBList) JSON.parse(custom);
        StringBuilder csv = new StringBuilder();
        for(Object documentObject: dbList){
            BasicDBObject document = (BasicDBObject) documentObject;
            BasicDBObject webRfqDocument = ((BasicDBObject)document.get("web_rfq"));
            csv.append("id:"+document.get("customer_id")+"|");
            CsvAppender.appendToCSV(csv, webRfqDocument, "vertical");
            CsvAppender.appendToCSV(csv, webRfqDocument, "primary_trade");
            CsvAppender.appendToCSV(csv, webRfqDocument, "annual_turnover");
            CsvAppender.appendToCSV(csv, webRfqDocument, "selected_insurer");
            csv.append("\n");
        }
        exchange.getIn().setBody(csv.toString());
    }
}

package clustercustomers.camel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mongodb.MongoDbConstants;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 27/05/2014
 * Time: 12:51
 * To change this template use File | Settings | File Templates.
 */
public class Routes extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        from("direct:findAll")
                //.setBody().constant("{ \"first_name\": \"Dfgb\" }")
                .to("mongodb:mongo?database=chopin_development&collection=customers&operation=findAll")
                .process(new CustomerMongoPersonalToSequenceFileProcessor())
                .to("hdfs://192.168.1.10:9000/chopin_customers?append=false&overwrite=true&fileType=NORMAL_FILE");

        from("direct:findAllWebRfq")
                //.setBody().constant("{ \"first_name\": \"Dfgb\" }")
                .setHeader(MongoDbConstants.LIMIT).constant(100)
                .to("mongodb:mongo?database=chopin_development&collection=rfqs&operation=findAll")
                .process(new CustomerMongoWebRfqToSequenceFile())
                .to("hdfs://192.168.1.10:9000/chopin_customers_rfqs?append=false&overwrite=true&fileType=NORMAL_FILE");
    }
}

package clustercustomers.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 27/05/2014
 * Time: 12:47
 * To change this template use File | Settings | File Templates.
 */
public class Routing {
    public static void main( String[] args )
    {
        ApplicationContext ac = new ClassPathXmlApplicationContext("context.xml");
        CamelContext context = ac.getBean(CamelContext.class);
        ProducerTemplate template = context.createProducerTemplate();

        template.sendBody("direct:findAll", "");
        template.sendBody("direct:findAllWebRfq", "");
    }
}

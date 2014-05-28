package clustercustomers.camel;

import com.mongodb.BasicDBObject;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 28/05/2014
 * Time: 15:13
 * To change this template use File | Settings | File Templates.
 */
public class CsvAppender {
    public static void appendToCSV(StringBuilder csv, BasicDBObject document, String field){
        csv.append(field +":"+document.get(field)).append(",");
    }
}

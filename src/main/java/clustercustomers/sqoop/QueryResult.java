// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Thu May 29 17:08:56 BST 2014
// For connector: org.apache.sqoop.manager.GenericJdbcManager
package clustercustomers.sqoop;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
    private final int PROTOCOL_VERSION = 3;
    public int getClassFormatVersion() { return PROTOCOL_VERSION; }
    protected ResultSet __cur_result_set;
    private String __id;
    public String get___id() {
        return __id;
    }
    public void set___id(String __id) {
        this.__id = __id;
    }
    public QueryResult with___id(String __id) {
        this.__id = __id;
        return this;
    }
    private String product;
    public String get_product() {
        return product;
    }
    public void set_product(String product) {
        this.product = product;
    }
    public QueryResult with_product(String product) {
        this.product = product;
        return this;
    }
    private String primary_trade;
    public String get_primary_trade() {
        return primary_trade;
    }
    public void set_primary_trade(String primary_trade) {
        this.primary_trade = primary_trade;
    }
    public QueryResult with_primary_trade(String primary_trade) {
        this.primary_trade = primary_trade;
        return this;
    }
    private String annual_turnover;
    public String get_annual_turnover() {
        return annual_turnover;
    }
    public void set_annual_turnover(String annual_turnover) {
        this.annual_turnover = annual_turnover;
    }
    public QueryResult with_annual_turnover(String annual_turnover) {
        this.annual_turnover = annual_turnover;
        return this;
    }
    private String claim_count;
    public String get_claim_count() {
        return claim_count;
    }
    public void set_claim_count(String claim_count) {
        this.claim_count = claim_count;
    }
    public QueryResult with_claim_count(String claim_count) {
        this.claim_count = claim_count;
        return this;
    }
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QueryResult)) {
            return false;
        }
        QueryResult that = (QueryResult) o;
        boolean equal = true;
        equal = equal && (this.__id == null ? that.__id == null : this.__id.equals(that.__id));
        equal = equal && (this.product == null ? that.product == null : this.product.equals(that.product));
        equal = equal && (this.primary_trade == null ? that.primary_trade == null : this.primary_trade.equals(that.primary_trade));
        equal = equal && (this.annual_turnover == null ? that.annual_turnover == null : this.annual_turnover.equals(that.annual_turnover));
        equal = equal && (this.claim_count == null ? that.claim_count == null : this.claim_count.equals(that.claim_count));
        return equal;
    }
    public void readFields(ResultSet __dbResults) throws SQLException {
        this.__cur_result_set = __dbResults;
        this.__id = JdbcWritableBridge.readString(1, __dbResults);
        this.product = JdbcWritableBridge.readString(2, __dbResults);
        this.primary_trade = JdbcWritableBridge.readString(3, __dbResults);
        this.annual_turnover = JdbcWritableBridge.readString(4, __dbResults);
        this.claim_count = JdbcWritableBridge.readString(5, __dbResults);
    }
    public void loadLargeObjects(LargeObjectLoader __loader)
            throws SQLException, IOException, InterruptedException {
    }
    public void write(PreparedStatement __dbStmt) throws SQLException {
        write(__dbStmt, 0);
    }

    public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
        JdbcWritableBridge.writeString(__id, 1 + __off, 12, __dbStmt);
        JdbcWritableBridge.writeString(product, 2 + __off, 12, __dbStmt);
        JdbcWritableBridge.writeString(primary_trade, 3 + __off, 12, __dbStmt);
        JdbcWritableBridge.writeString(annual_turnover, 4 + __off, 12, __dbStmt);
        JdbcWritableBridge.writeString(claim_count, 5 + __off, 12, __dbStmt);
        return 5;
    }
    public void readFields(DataInput __dataIn) throws IOException {
        if (__dataIn.readBoolean()) {
            this.__id = null;
        } else {
            this.__id = Text.readString(__dataIn);
        }
        if (__dataIn.readBoolean()) {
            this.product = null;
        } else {
            this.product = Text.readString(__dataIn);
        }
        if (__dataIn.readBoolean()) {
            this.primary_trade = null;
        } else {
            this.primary_trade = Text.readString(__dataIn);
        }
        if (__dataIn.readBoolean()) {
            this.annual_turnover = null;
        } else {
            this.annual_turnover = Text.readString(__dataIn);
        }
        if (__dataIn.readBoolean()) {
            this.claim_count = null;
        } else {
            this.claim_count = Text.readString(__dataIn);
        }
    }
    public void write(DataOutput __dataOut) throws IOException {
        if (null == this.__id) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, __id);
        }
        if (null == this.product) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, product);
        }
        if (null == this.primary_trade) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, primary_trade);
        }
        if (null == this.annual_turnover) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, annual_turnover);
        }
        if (null == this.claim_count) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, claim_count);
        }
    }
    private final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
    public String toString() {
        return toString(__outputDelimiters, true);
    }
    public String toString(DelimiterSet delimiters) {
        return toString(delimiters, true);
    }
    public String toString(boolean useRecordDelim) {
        return toString(__outputDelimiters, useRecordDelim);
    }
    public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
        StringBuilder __sb = new StringBuilder();
        char fieldDelim = delimiters.getFieldsTerminatedBy();
        __sb.append(FieldFormatter.escapeAndEnclose(__id==null?"null":__id, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(product==null?"null":product, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(primary_trade==null?"null":primary_trade, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(annual_turnover==null?"null":annual_turnover, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(claim_count==null?"null":claim_count, delimiters));
        if (useRecordDelim) {
            __sb.append(delimiters.getLinesTerminatedBy());
        }
        return __sb.toString();
    }
    private final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
    private RecordParser __parser;
    public void parse(Text __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    public void parse(CharSequence __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    public void parse(byte [] __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    public void parse(char [] __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    public void parse(ByteBuffer __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    public void parse(CharBuffer __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    private void __loadFromFields(List<String> fields) {
        Iterator<String> __it = fields.listIterator();
        String __cur_str;
        __cur_str = __it.next();
        if (__cur_str.equals("null")) { this.__id = null; } else {
            this.__id = __cur_str;
        }

        __cur_str = __it.next();
        if (__cur_str.equals("null")) { this.product = null; } else {
            this.product = __cur_str;
        }

        __cur_str = __it.next();
        if (__cur_str.equals("null")) { this.primary_trade = null; } else {
            this.primary_trade = __cur_str;
        }

        __cur_str = __it.next();
        if (__cur_str.equals("null")) { this.annual_turnover = null; } else {
            this.annual_turnover = __cur_str;
        }

        __cur_str = __it.next();
        if (__cur_str.equals("null")) { this.claim_count = null; } else {
            this.claim_count = __cur_str;
        }

    }

    public Object clone() throws CloneNotSupportedException {
        QueryResult o = (QueryResult) super.clone();
        return o;
    }

    public Map<String, Object> getFieldMap() {
        Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
        __sqoop$field_map.put("__id", this.__id);
        __sqoop$field_map.put("product", this.product);
        __sqoop$field_map.put("primary_trade", this.primary_trade);
        __sqoop$field_map.put("annual_turnover", this.annual_turnover);
        __sqoop$field_map.put("claim_count", this.claim_count);
        return __sqoop$field_map;
    }

    public void setField(String __fieldName, Object __fieldVal) {
        if ("__id".equals(__fieldName)) {
            this.__id = (String) __fieldVal;
        }
        else    if ("product".equals(__fieldName)) {
            this.product = (String) __fieldVal;
        }
        else    if ("primary_trade".equals(__fieldName)) {
            this.primary_trade = (String) __fieldVal;
        }
        else    if ("annual_turnover".equals(__fieldName)) {
            this.annual_turnover = (String) __fieldVal;
        }
        else    if ("claim_count".equals(__fieldName)) {
            this.claim_count = (String) __fieldVal;
        }
        else {
            throw new RuntimeException("No such field: " + __fieldName);
        }
    }
}

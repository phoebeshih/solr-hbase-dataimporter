package org.apache.solr.handler.dataimport;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by phoebe.shih on 2017/5/13.
 */
public class HBaseResultSet {
    public static final int INT = 0;
    public static final int LONG = 1;
    public static final int DOUBLE =2;
    public static final int FLOAT = 3;
    public static final int STRING = 4;
    public static final int BOOLEAN = 5;
    public static final int DATE = 6;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseResultSet.class);

    private static final String COLUMNS = "columns";
    protected static final String ROWKEY = "rowkey";

    private ResultScanner scanner;
    private Iterator<Map<String, Object>> iterator;
    private Iterator<Result> resultIterator;

    private List<HBaseColumn> columns;

    public HBaseResultSet(Context context, ResultScanner scanner){
        this.scanner = scanner;
        this.columns = HBaseColumn.toList(COLUMNS, context);
        this.resultIterator = scanner.iterator();

        iterator = new Iterator<Map<String, Object>>() {
            public boolean hasNext() {
                return doHasNext();
            }

            public Map<String, Object> next() {
                return doNext();
            }

            public void remove() {
            }
        };
    }

    public Iterator<Map<String, Object>> getIterator(){
        return iterator;
    }

    private boolean doHasNext(){
        if(resultIterator == null){
            return false;
        }
        if(resultIterator.hasNext()){
            return true;
        }
        this.close();
        return false;
    }

    private void close(){
        if(scanner == null){
            return;
        }
        scanner.close();
    }

    private Map<String, Object> doNext(){
        Map<String, Object> row = new HashMap<String, Object>();
        Result result = resultIterator.next();
        this.putValue(columns, result, row);
        if(!row.isEmpty()) {
            row.put(ROWKEY, Bytes.toString(result.getRow()));
        }
        return row;
    }

    /**
     * implement detail of how to put value
     */
    protected void putValue(List<HBaseColumn> columns, Result result, Map<String, Object> row){
        for(HBaseColumn column : columns){
            String familyName = column.getFamily();
            String cqName = column.getCq();
            byte[] value = result.getValue(familyName.getBytes(), cqName.getBytes());
            Object obj = DataType.converToObject(column.getType(), value);
            row.put(cqName, obj);
        }
    }
}

package org.apache.solr.handler.dataimport;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by phoebe.shih on 2017/5/13.
 */
public class HBaseScanFactory {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseScanFactory.class);
    private static final String START_TIME = "startTime";
    private static final String COLUMNS = "columns";
    private static final String REQUIRED = "required";
    private Context context;
    public HBaseScanFactory(Context context){
        this.context = context;
    }

    public Scan create(){
        Scan scan = new Scan();
        this.addColumns(scan);
        this.setRequired(scan);
        this.setTimeRange(scan);
        return scan;
    }

    private void setTimeRange(Scan scan){
        LOG.debug("\t===context.currentProcess():{}===", context.currentProcess());
        String time = context.getEntityAttribute(START_TIME);
        if(context.currentProcess().equals(Context.FIND_DELTA)){
            time = context.replaceTokens(time);
        }

        LOG.debug("\t===startTime:{}===", time);
        if(time==null || time.length() == 0){
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            long startTime = sdf.parse(time).getTime();
            long endTime = System.currentTimeMillis();
            scan.setTimeRange(startTime, endTime);
        }catch(Exception e){
            LOG.warn("\t===invalid time:{}===", time);
            //throw new DataImportException(e);
        }
    }

    private void addColumns(Scan scan){
        List<HBaseColumn> columns = HBaseColumn.toList(COLUMNS, context);
        for(HBaseColumn column: columns){
            scan.addColumn(column.getFamily().getBytes(), column.getCq().getBytes());
        }
    }

    private void setRequired(Scan scan){
        List<HBaseColumn> columns = HBaseColumn.toList(REQUIRED, context);
        FilterList filters = new FilterList();
        for(HBaseColumn column: columns){
            SingleColumnValueFilter filter = new SingleColumnValueFilter(column.getFamily().getBytes()
                    , column.getCq().getBytes()
                    , CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(""));
            filter.setFilterIfMissing(true);
            filters.addFilter(filter);
        }
        scan.setFilter(filters);
    }
}

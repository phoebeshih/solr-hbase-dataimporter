package org.apache.solr.handler.dataimport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by phoebe.shih on 2017/5/13.
 */
public class HBaseColumn {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseColumn.class);
    private static final String COLUMN_DIVIDER = ",";
    private static final String FAMILY_DIVIDER = ":";
    private static final int INDEX_CQ = 1;
    private static final int INDEX_FAMILY =0;

    private String family;
    private String cq;
    private int type = -1;

    public int getType() {
        return type;
    }

    public HBaseColumn setType(int type) {
        this.type = type;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public HBaseColumn setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getCq() {
        return cq;
    }

    public HBaseColumn setCq(String cq) {
        this.cq = cq;
        return this;
    }


    public static List<HBaseColumn> toList(String fieldName, Context context){
        Map<String, HBaseColumn> cqMap = new HashMap<String, HBaseColumn>();
        //init columns
        String[] fields = context.getEntityAttribute(fieldName).split(COLUMN_DIVIDER);
        for(String field: fields){
            String[] tmp = field.split(FAMILY_DIVIDER);
            if(tmp.length!=2){
                LOG.error("\t===invalid column:{}===", field);
                continue;
            }
            cqMap.put(tmp[INDEX_CQ], new HBaseColumn()
                    .setFamily(tmp[INDEX_FAMILY])
                    .setCq(tmp[INDEX_CQ]));
        }

        //init column data type
        for (Map<String, String> map : context.getAllEntityFields()) {
            String cq = map.get(DataImporter.COLUMN);
            if(HBaseResultSet.ROWKEY.equals(cq)){
                continue;
            }

            String type = map.get(DataImporter.TYPE);
            for(DataType dataType: DataType.values()){
                if(dataType.valid(type)){
                    HBaseColumn column = cqMap.get(cq);
                    if(column == null){
                        LOG.warn("\t===not defined in cloumns:{}===", cq);
                        continue;
                    }
                    column.setType(dataType.getKey());
                    cqMap.put(cq, column);
                }
            }
        }

        return new ArrayList<HBaseColumn>(cqMap.values());
    }
}

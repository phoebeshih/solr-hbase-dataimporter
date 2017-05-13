package org.apache.solr.handler.dataimport;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * Created by phoebe.shih on 2017/5/13.
 */
public enum  DataType {
    INT(HBaseResultSet.INT, "int", "tint"),
    LONG(HBaseResultSet.LONG, "long", "tlong"),
    DOUBLE(HBaseResultSet.DOUBLE, "double", "tdouble"),
    FLOAT(HBaseResultSet.FLOAT, "float", "tfloat"),
    STRING(HBaseResultSet.STRING, "string", "text_general"),
    BOOLEAN(HBaseResultSet.BOOLEAN, "boolean"),
    DATE(HBaseResultSet.DATE, "date", "tdate");

    private int key;
    private List<String> types;

    DataType(int key, String... types){
        this.key = key;
        this.types = Arrays.asList(types);
    }

    public int getKey(){
        return key;
    }

    public boolean valid(String type){
        return types.contains(type);
    }

    public static Object converToObject(int type, byte[] value){
        switch(type){
            case HBaseResultSet.INT:
                return Bytes.toInt(value);
            case HBaseResultSet.LONG:
                return Bytes.toLong(value);
            case HBaseResultSet.DOUBLE:
                return Bytes.toDouble(value);
            case HBaseResultSet.FLOAT:
                return Bytes.toFloat(value);
            case HBaseResultSet.STRING:
                return Bytes.toString(value);
            case HBaseResultSet.BOOLEAN:
                return Bytes.toBoolean(value);
            case HBaseResultSet.DATE:
                return new Date(Bytes.toLong(value));
            default:
                return Bytes.toString(value);
        }
    }
}

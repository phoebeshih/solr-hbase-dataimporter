package org.apache.solr.handler.dataimport;

/**
 * Created by phoebe.shih on 2017/5/13.
 */
public class DataImportException extends RuntimeException{

    public DataImportException(String errorMessage){
        super(errorMessage);
    }

    public DataImportException(Throwable e){
        super(e);
    }
}

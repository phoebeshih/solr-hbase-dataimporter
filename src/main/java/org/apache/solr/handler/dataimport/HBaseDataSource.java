package org.apache.solr.handler.dataimport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by phoebe.shih on 2017/5/13.
 */
public class HBaseDataSource extends DataSource<Iterator<Map<String, Object>>>{
    private static final Logger LOG = LoggerFactory.getLogger(HBaseDataSource.class);
    private static final String ZOOKEEPER = "zookeeper";
    private static final String ROOT_DIR = "rootDir";
    private static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_ROOT_DIR = "hbase.root.dir";
    private static final String TABLE = "table";

    private Connection connection;
    private Table table;
    private Context context;

    @Override
    public void init(Context context, Properties properties) {
        this.context = context;

        String zooKeeperQuorum = (String) properties.get(ZOOKEEPER);
        String hdfsRootDir = (String) properties.get(ROOT_DIR);
        String tableName = context.getEntityAttribute(TABLE);
        if(zooKeeperQuorum==null || hdfsRootDir == null){
            throw new DataImportException(String.format("required value is missing %s:%s or %s:%s"
                    , ZOOKEEPER, zooKeeperQuorum
                    , ROOT_DIR, hdfsRootDir));
        }

        if(tableName == null){
            throw new DataImportException("required value is missing: table");
        }

        Configuration config = HBaseConfiguration.create();
        config.set(HBASE_ZOOKEEPER_QUORUM, zooKeeperQuorum);
        config.set(HBASE_ROOT_DIR, hdfsRootDir);
        try {
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));
        }catch (IOException e){
            throw new DataImportException(e);
        }
    }

    @Override
    public Iterator<Map<String, Object>> getData(String query) {
        try {
            ResultScanner scanner = table.getScanner(this.createScan(context));
            return new HBaseResultSet(context, scanner).getIterator();
        }catch(IOException e){
            throw new DataImportException(e);
        }
    }

    /**
     * implement detail of scan with context
     * */
    protected Scan createScan(Context context){
        return new HBaseScanFactory(context).create();
    }

    @Override
    public void close() {
        try {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    throw new DataImportException(e);
                }
            }
        }finally {
            if(connection!=null){
                try {
                    connection.close();
                }catch(IOException e){
                    throw new DataImportException(e);
                }
            }
        }

    }
}

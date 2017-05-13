# solr-hbase-dataimporter

## solrconfig.xml
    <requestHandler name="/dataimport" class="org.apache.solr.handler.dataimport.DataImportHandler">
        <lst name="defaults">
          <str name="config">data-config.xml</str>
          <str name="clean">false</str> 
          <str name="commit">true</str> 
          <str name="verbose">true</str> 
        </lst>
    </requestHandler>  

## data-config.xml
    <dataConfig>
      <dataSource name="HBase" type="HBaseDataSource" 
            rootDir="hdfs://localhost:8020/hbase" 
            zookeeper="localhost"/>
      <document>
        <entity name="hbase" dataSource="HBase" processor="HBaseEntityProcessor" 
          table ="Test" columns="f1:cq1,f1:cq2" required="f1:cq1,f1:cq2">
          <field column="rowkey" name="id" />
          <field column="cq1" name="columnQualifier1" />
          <field column="cq2" name="columnQualifier2" />
        </entity>
      </document>
    </dataConfig>

## HBaseDataSource
* rootDir: Hbase Root Directory
* zookeeper: Hbase Zookeeper

## HBaseEntityProcessor
* table: hbase table name
* columns: column family and column qualifier
* required: required column for scan filter
* rowkey: hbase rowkey

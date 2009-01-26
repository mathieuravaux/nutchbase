package org.apache.nutchbase.util.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class WebTableCreator {

  public static void main(String args[]) throws IOException {

    HBaseConfiguration hbaseConf = new HBaseConfiguration();

    HTableDescriptor desc = new HTableDescriptor("webtable");
    
    desc.addFamily(new HColumnDescriptor(TableColumns.BASE_URL));
    desc.addFamily(new HColumnDescriptor(TableColumns.STATUS));
    desc.addFamily(new HColumnDescriptor(TableColumns.FETCH_TIME));
    desc.addFamily(new HColumnDescriptor(TableColumns.RETRIES));
    desc.addFamily(new HColumnDescriptor(TableColumns.FETCH_INTERVAL));
    desc.addFamily(new HColumnDescriptor(TableColumns.SCORE));
    desc.addFamily(new HColumnDescriptor(TableColumns.MODIFIED_TIME));
    desc.addFamily(new HColumnDescriptor(TableColumns.SIGNATURE));
    desc.addFamily(new HColumnDescriptor(TableColumns.CONTENT));
    desc.addFamily(new HColumnDescriptor(TableColumns.CONTENT_TYPE));
    desc.addFamily(new HColumnDescriptor(TableColumns.TITLE));
    desc.addFamily(new HColumnDescriptor(TableColumns.OUTLINKS));
    desc.addFamily(new HColumnDescriptor(TableColumns.INLINKS));
    desc.addFamily(new HColumnDescriptor(TableColumns.PARSE_STATUS));
    desc.addFamily(new HColumnDescriptor(TableColumns.PROTOCOL_STATUS));
    desc.addFamily(new HColumnDescriptor(TableColumns.TEXT));
    desc.addFamily(new HColumnDescriptor(TableColumns.REPR_URL));
    desc.addFamily(new HColumnDescriptor(TableColumns.HEADERS));
    desc.addFamily(new HColumnDescriptor(TableColumns.METADATA));

    // Hackish solution to access previous versions of some columns
    desc.addFamily(new HColumnDescriptor(TableColumns.PREV_SIGNATURE));
    desc.addFamily(new HColumnDescriptor(TableColumns.PREV_FETCH_TIME));

    HBaseAdmin admin = new HBaseAdmin(hbaseConf);

    admin.createTable(desc);
  }
}

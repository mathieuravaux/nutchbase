package org.apache.nutchbase.util.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.*;

import org.apache.nutch.util.NutchConfiguration;

public class WebTableCreator extends Configured implements Tool {

    public static final Log LOG = LogFactory.getLog(WebTableCreator.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(),
                new WebTableCreator(), args);
        System.exit(res);
    }

    /**
     * Adds the required column families to the target table. This method does commit any changes to the table it
     * simply adds the descriptors required.
     *
     * @param targetTable the HTableDescriptor for the table you wish to add the webtable column families.
     */
    public static void addColumnFamilies(HTableDescriptor targetTable) {
        targetTable.addFamily(new HColumnDescriptor(TableColumns.BASE_URL));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.STATUS));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.FETCH_TIME));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.RETRIES));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.FETCH_INTERVAL));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.SCORE));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.MODIFIED_TIME));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.SIGNATURE));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.CONTENT));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.CONTENT_TYPE));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.TITLE));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.OUTLINKS));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.INLINKS));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.PARSE_STATUS));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.PROTOCOL_STATUS));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.TEXT));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.REPR_URL));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.HEADERS));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.METADATA));

        // Hackish solution to access previous versions of some columns
        targetTable.addFamily(new HColumnDescriptor(TableColumns.PREV_SIGNATURE));
        targetTable.addFamily(new HColumnDescriptor(TableColumns.PREV_FETCH_TIME));

    }

    public int run(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: WebTableCreator <webtable>");
            return -1;
        }
        try {

            HBaseConfiguration hbaseConf = new HBaseConfiguration();

            LOG.debug("Creating table: " + args[0]);
            HTableDescriptor desc = new HTableDescriptor(args[0]);
            addColumnFamilies(desc);

            HBaseAdmin admin = new HBaseAdmin(hbaseConf);
            LOG.warn("Calling createTable");
            admin.createTable(desc);
            return 0;
        } catch (Exception e) {
            LOG.fatal("WebTableCreator: " + StringUtils.stringifyException(e));
            return -1;
        }

    }
}
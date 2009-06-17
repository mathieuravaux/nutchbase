package org.apache.nutchbase.util.hbase;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
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

public int run(String[] args) throws Exception {

	if (args.length != 1) {
		System.err.println("Usage: WebTableCreator <webtable>");
		return -1;
	}
	try {
	
    HBaseConfiguration hbaseConf = new HBaseConfiguration();

		LOG.debug("Creating table: " + args[0]);
    HTableDescriptor desc = new HTableDescriptor(args[0]);
    
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

    desc.addFamily(new HColumnDescriptor(TableColumns.PAGERANK));
    desc.addFamily(new HColumnDescriptor(TableColumns.VOTES));

    // Hackish solution to access previous versions of some columns
    desc.addFamily(new HColumnDescriptor(TableColumns.PREV_SIGNATURE));
    desc.addFamily(new HColumnDescriptor(TableColumns.PREV_FETCH_TIME));

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

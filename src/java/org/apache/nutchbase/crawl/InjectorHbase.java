package org.apache.nutchbase.crawl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutchbase.util.hbase.ImmutableRowPart;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.nutchbase.util.hbase.TableColumns;
import org.apache.nutchbase.util.hbase.TableMapReduce;
import org.apache.nutchbase.util.hbase.TableUtil;

public class InjectorHbase
extends TableMapReduce<ImmutableBytesWritable, BooleanWritable>
implements Tool {

  public static final Log LOG = LogFactory.getLog(InjectorHbase.class);
  
  private static final String INJECT_KEY_STR = "__tmp_inject_key__";
  private static final String META_INJECT_KEY_STR =
    TableColumns.METADATA_STR + INJECT_KEY_STR;
  private static final byte[] META_INJECT_KEY =
    Bytes.toBytes(META_INJECT_KEY_STR);
  
  private static final Set<String> COLUMNS = new HashSet<String>();
  
  static {
    COLUMNS.add(META_INJECT_KEY_STR);
    COLUMNS.add(TableColumns.STATUS_STR);
  }

  private int interval;
  private float scoreInjected;
  private long curTime;
  private ImmutableRowPart row = new ImmutableRowPart();

  public static class UrlMapperHbase 
  implements Mapper<LongWritable, Text, Text, Text> {
    private URLNormalizers urlNormalizers;
    private URLFilters filters;
    private HTable table;
    private HBaseConfiguration hbaseConf;

    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
    throws IOException {
      if (table == null) {
        throw new IOException("Can not connect to hbase table");
      }
      String url = value.toString();
      String reversedUrl;
      try {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        url = filters.filter(url);
        if (url == null) {
          return;
        }
        reversedUrl = TableUtil.reverseUrl(url);
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        return;
      }

      BatchUpdate bu = new BatchUpdate(reversedUrl);
      bu.put(META_INJECT_KEY, TableUtil.YES_VAL);

      table.commit(bu);
    }

    public void configure(JobConf job) {  
      urlNormalizers = new URLNormalizers(job, URLNormalizers.SCOPE_INJECT);
      filters = new URLFilters(job);
      hbaseConf = new HBaseConfiguration();
      try {
        table = new HTable(hbaseConf, job.get("input.table") );
      } catch (IOException e) {
        e.printStackTrace(LogUtil.getFatalStream(LOG));
      }

    }

    public void close() throws IOException { }

  }

  @Override
  public void map(ImmutableBytesWritable key, RowResult rowResult,
      OutputCollector<ImmutableBytesWritable, BooleanWritable> output,
      Reporter reporter)
  throws IOException {
    row = new ImmutableRowPart(rowResult);
    if (!row.hasMeta(INJECT_KEY_STR)) {
      return;
    }

    output.collect(key, new BooleanWritable(
        row.hasColumn(TableColumns.STATUS)));
  }

  public void configure(JobConf job) {
    interval = job.getInt("db.fetch.interval.default", 2592000);
    scoreInjected = job.getFloat("db.score.injected", 1.0f);
    curTime = job.getLong("injector.current.time", System.currentTimeMillis());
  }

  @Override
  public void reduce(ImmutableBytesWritable key,
      Iterator<BooleanWritable> values,
      OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
      Reporter reporter)
  throws IOException {
    boolean isOld = values.next().get();

    RowPart row = new RowPart(key.get());
    row.deleteMeta(INJECT_KEY_STR);

    if (!isOld) {
      row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
      row.setFetchTime(curTime);
      row.setFetchInterval(interval);
      row.setScore(scoreInjected);
      row.setPagerank(0.0f);
      row.setVotes(0.0f);
      row.setRetriesSinceFetch(0);
    }

    output.collect(key, row.makeBatchUpdate());
  }

  public void inject(String table, Path urlDir) throws IOException {

    LOG.info("InjectorHbase: starting");
    LOG.info("InjectorHbase: urlDir: " + urlDir);

    JobConf job = new NutchJob(getConf());
    job.setJobName("inject-hbase-p1 " + urlDir);
    FileInputFormat.addInputPath(job, urlDir);
    job.setMapperClass(UrlMapperHbase.class);

    job.setOutputFormat(NullOutputFormat.class);
    job.setLong("injector.current.time", System.currentTimeMillis());
    job.set("input.table", table);
    JobClient.runJob(job);

    job = new NutchJob(getConf());

    job.setJobName("inject-hbase-p2 " + urlDir);
    TableMapReduce.initJob(table, 
        TableUtil.getColumns(COLUMNS),
        InjectorHbase.class,
        ImmutableBytesWritable.class,
        BooleanWritable.class, job);

    JobClient.runJob(job);

    LOG.info("InjectorHbase: done");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: InjectorHbase <webtable> <url_dir>");
      return -1;
    }
    try {
      inject(args[0], new Path(args[1]));
      return 0;
    } catch (Exception e) {
      LOG.fatal("InjectorHbase: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(),
        new InjectorHbase(), args);
    System.exit(res);
  }
}

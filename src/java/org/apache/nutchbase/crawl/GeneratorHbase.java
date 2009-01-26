package org.apache.nutchbase.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.nutchbase.util.hbase.TableColumns;
import org.apache.nutchbase.util.hbase.TableMapReduce;
import org.apache.nutchbase.util.hbase.TableUtil;

public class GeneratorHbase
extends Configured
implements Tool {
  private static final String CRAWL_GENERATE_FILTER = "crawl.generate.filter";
  private static final String GENERATE_MAX_PER_HOST = "generate.max.per.host";
  private static final String CRAWL_TOP_N = "crawl.topN";
  private static final String CRAWL_GEN_CUR_TIME = "crawl.gen.curTime";
  private static final String CRAWL_RANDOM_SEED = "generate.partition.seed";
  
  private static final Set<String> COLUMNS = new HashSet<String>();
  
  static {
    COLUMNS.add(TableColumns.FETCH_TIME_STR);
    COLUMNS.add(TableColumns.SCORE_STR);
    COLUMNS.add(TableColumns.STATUS_STR);
  }
  
  public static final String TMP_FETCH_MARK = "__tmp_fetch_mark__";
  
  public static final Log LOG = LogFactory.getLog(GeneratorHbase.class);

  public static class SelectorEntryHbase
  implements WritableComparable<SelectorEntryHbase> {
    private String reversedHost;
    private float score;
    
    public SelectorEntryHbase() {
      reversedHost = "";
    }
    
    public SelectorEntryHbase(String reversedHost, float score) {
      this.reversedHost = reversedHost;
      this.score = score;
    }

    public void readFields(DataInput in) throws IOException {
      reversedHost = Text.readString(in);
      score = in.readFloat();
    }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, reversedHost);
      out.writeFloat(score);
    }

    public int compareTo(SelectorEntryHbase se) {
      int hostCompare = reversedHost.compareTo(se.reversedHost);
      if (hostCompare == 0) {
        if (se.score > score)
          return 1;
        else if (se.score == score)
          return 0;
        return -1;
      }
      return hostCompare;
    }
  }

  public static class SelectorEntryHbaseComparator extends WritableComparator {
    public SelectorEntryHbaseComparator() {
      super(SelectorEntryHbase.class, true);
    }
  }
  
  static {
    WritableComparator.define(SelectorEntryHbase.class,
                              new SelectorEntryHbaseComparator());
  }
  
  public static class GeneratorMapReduce
  extends TableMapReduce<SelectorEntryHbase, RowPart>
  implements Partitioner<SelectorEntryHbase, RowPart> {

    private long curTime;
    private long limit;
    private long hostCount;
    private long count;
    private long maxPerHost;
    private int seed;
    private URLFilters filters;
    private URLNormalizers normalizers;
    private boolean filter;
    private String prevReversedHost = "";
    private FetchScheduleHbase schedule;

    @Override
    public void map(ImmutableBytesWritable key, RowResult rowResult,
        OutputCollector<SelectorEntryHbase, RowPart> output,
        Reporter reporter) throws IOException {
      String reversedUrl = Bytes.toString(key.get());
      String url = TableUtil.unreverseUrl(reversedUrl);
      
      RowPart row = new RowPart(rowResult);
      
      // If filtering is on don't generate URLs that don't pass URLFilters
      try {
        url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
        if (filter && filters.filter(url) == null)
          return;
      } catch (URLFilterException e) {
        LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
        return;
      }
            
      // check fetch schedule
      if (!schedule.shouldFetch(url, row, curTime)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("-shouldFetch rejected '" + url + "', fetchTime=" + 
                    row.getFetchTime() + ", curTime=" + curTime);
        }
        return;
      }
      
      float score = row.getScore();
      SelectorEntryHbase entry =
        new SelectorEntryHbase(TableUtil.getReversedHost(reversedUrl), score);
      output.collect(entry, row);

    }

    @Override
    public void reduce(SelectorEntryHbase key, Iterator<RowPart> values,
        OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
        Reporter reporter) throws IOException {

      if (!key.reversedHost.equals(prevReversedHost)) { // new host
        hostCount = 0;
        prevReversedHost = key.reversedHost;
      }

      while (values.hasNext()) {
        if (count >= limit || hostCount > maxPerHost) {
          return;
        }
        RowPart row = values.next();
        ImmutableBytesWritable outKey =
          new ImmutableBytesWritable(row.getRowId());
        row.putMeta(TMP_FETCH_MARK, TableUtil.YES_VAL);
        output.collect(outKey, row.makeBatchUpdate());
        hostCount++;
        count++;
      }
    }  

    public void configure(JobConf job) {
      seed = job.getInt(CRAWL_RANDOM_SEED, 0);
      curTime = job.getLong(CRAWL_GEN_CUR_TIME, System.currentTimeMillis());
      limit = job.getLong(CRAWL_TOP_N,Long.MAX_VALUE)/job.getNumReduceTasks();
      maxPerHost = job.getInt(GENERATE_MAX_PER_HOST, -1);
      if (maxPerHost < 0) {
        maxPerHost = Long.MAX_VALUE;
      }
      filters = new URLFilters(job);
      normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
      filter = job.getBoolean(CRAWL_GENERATE_FILTER, true);
      schedule = FetchScheduleFactoryHbase.getFetchSchedule(job);
    }

    public int getPartition(SelectorEntryHbase key, RowPart value,
                            int numPartitions) {
      int hashCode = key.reversedHost.hashCode();

      // make hosts wind up in different partitions on different runs
      hashCode ^= seed;

      return (hashCode & Integer.MAX_VALUE) % numPartitions;  
     }
  }
  
  /**
   * Mark URLs ready for fetching.
   * */
  public void generate(String table, long topN, long curTime, boolean filter)
  throws IOException {    
 
    LOG.info("GeneratorHbase: Selecting best-scoring urls due for fetch.");
    LOG.info("GeneratorHbase: starting");
    LOG.info("GeneratorHbase: filtering: " + filter);
    if (topN != Long.MAX_VALUE) {
      LOG.info("GeneratorHbase: topN: " + topN);
    }
 
    // map to inverted subset due for fetch, sort by score
    JobConf job = new NutchJob(getConf());
    job.setJobName("generate-hbase: " + table);

    job.setLong(CRAWL_GEN_CUR_TIME, curTime);
    job.setLong(CRAWL_TOP_N, topN);
    job.setBoolean(CRAWL_GENERATE_FILTER, filter);
    job.setInt(CRAWL_RANDOM_SEED, new Random().nextInt());

    TableMapReduce.initJob(table, TableUtil.getColumns(COLUMNS), 
                           GeneratorMapReduce.class,
                           SelectorEntryHbase.class,
                           RowPart.class,
                           job);
    
    job.setPartitionerClass(GeneratorMapReduce.class);
    
    JobClient.runJob(job);
    
    LOG.info("GeneratorHbase: done");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage: GeneratorHbase <webtable> [-topN N] [-noFilter]");
      return -1;
    }
    
    String table = args[0];
    long curTime = System.currentTimeMillis();
    long topN = Long.MAX_VALUE;
    boolean filter = true;

    for (int i = 1; i < args.length; i++) {
      if ("-topN".equals(args[i])) {
        topN = Long.parseLong(args[++i]);
      } else if ("-noFilter".equals(args[i])) {
        filter = false;
      }
    }
    
    try {
      generate(table, topN, curTime, filter);
      return 0;
    } catch (Exception e) {
      LOG.fatal("GeneratorHbase: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new GeneratorHbase(), args);
    System.exit(res);
  }
}

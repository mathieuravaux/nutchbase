package org.apache.nutchbase.crawl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutchbase.fetcher.FetcherHbase;
import org.apache.nutchbase.parse.ParseTable;
import org.apache.nutchbase.util.hbase.ImmutableRowPart;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.nutchbase.util.hbase.TableColumns;
import org.apache.nutchbase.util.hbase.TableMapReduce;
import org.apache.nutchbase.util.hbase.TableUtil;

public class UpdateTable
extends TableMapReduce<ImmutableBytesWritable, NutchWritable>
implements Tool {

  public static final Log LOG = LogFactory.getLog(UpdateTable.class);
  
  private static final Set<String> COLUMNS = new HashSet<String>();

  private static final String ALL = "db.update.all";
  
  static {
    COLUMNS.add(TableColumns.OUTLINKS_STR);
    COLUMNS.add(TableColumns.INLINKS_STR);
    COLUMNS.add(TableColumns.STATUS_STR);
    COLUMNS.add(TableColumns.METADATA_STR + ParseTable.TMP_UPDATE_MARK);
    COLUMNS.add(TableColumns.METADATA_STR + FetcherHbase.REDIRECT_DISCOVERED);
    COLUMNS.add(TableColumns.RETRIES_STR);
    COLUMNS.add(TableColumns.FETCH_TIME_STR);
    COLUMNS.add(TableColumns.MODIFIED_TIME_STR);
    COLUMNS.add(TableColumns.FETCH_INTERVAL_STR);
    COLUMNS.add(TableColumns.PREV_FETCH_TIME_STR);
    COLUMNS.add(TableColumns.PREV_SIGNATURE_STR);
  }
  
  private int retryMax;
  private boolean additionsAllowed;
  private int maxInterval;
  private float scoreInjected;
  private FetchScheduleHbase schedule;
  private List<Inlink> inlinks = new ArrayList<Inlink>();
  private boolean updateAll;

  @Override
  public void configure(JobConf job) {
    retryMax = job.getInt("db.fetch.retry.max", 3);
    additionsAllowed = job.getBoolean(CrawlDb.CRAWLDB_ADDITIONS_ALLOWED, true);
    scoreInjected = job.getFloat("db.score.injected", 1.0f);
    maxInterval = job.getInt("db.fetch.interval.max", 0 );
	updateAll = job.getBoolean("db.update.all", false);
	schedule = FetchScheduleFactoryHbase.getFetchSchedule(job);
  }

  @Override
  public void map(ImmutableBytesWritable key, RowResult rowResult,
      OutputCollector<ImmutableBytesWritable, NutchWritable> output,
      Reporter reporter)
  throws IOException {
	  
	ImmutableRowPart row = new ImmutableRowPart(rowResult);
    
	if (updateAll || row.hasMeta(ParseTable.TMP_UPDATE_MARK)) { 
	  
      output.collect(key, new NutchWritable(rowResult));

      Collection<Outlink> outlinks = row.getOutlinks();
    
      if (outlinks.isEmpty()) {
        return;
      }
    
      String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
    
      for (Outlink outlink : outlinks) {
        try {
          String reversedOut = TableUtil.reverseUrl(outlink.getToUrl());
          ImmutableBytesWritable outKey =
            new ImmutableBytesWritable(reversedOut.getBytes());
          output.collect(outKey, new NutchWritable(new Inlink(url, outlink.getAnchor())));
        } catch (Exception e) {
          // Catching anything isn't usually good - but we should report it and shouldn't crash the process for a bad URL. 
          LOG.info("Exception thrown by url: " + outlink.getToUrl().toString(), e);
        }
      }
	}
  }

  @Override
  public void reduce(ImmutableBytesWritable key,
      Iterator<NutchWritable> values,
      OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
      Reporter reporter) throws IOException {
  
    RowResult rowResult = null;
    inlinks.clear();
    
    while (values.hasNext()) {
      Writable val = values.next().get();
      if (val instanceof RowResult) {
        rowResult = (RowResult) val;
      } else {
        Inlink anchor = (Inlink) val;
        inlinks.add(anchor);
      }
    }
    String url;
    try {
       url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
    } catch (Exception e) {
      // this can happen because a newly discovered malformed link
      // may slip by url filters
      // TODO: Find a better solution
      return;
    }
    
    RowPart row;
    if (rowResult == null) { // new row
      if (!additionsAllowed) {
        return;
      }
      row = new RowPart(key.get());
      schedule.initializeSchedule(url, row);
      row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
      row.setScore(scoreInjected);
    } else {
      row = new RowPart(rowResult);
      if (row.hasMeta(FetcherHbase.REDIRECT_DISCOVERED) && !row.hasColumn(TableColumns.STATUS)) {
        // this row is marked during fetch as the destination of a redirect
        // but does not contain anything else, so we initialize it.
        schedule.initializeSchedule(url, row);
        row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
        row.setScore(scoreInjected);
      } else if (row.hasMeta(ParseTable.TMP_UPDATE_MARK)) { // marked for update
        byte status = row.getStatus();
        switch (status) {
        case CrawlDatumHbase.STATUS_FETCHED:         // succesful fetch
        case CrawlDatumHbase.STATUS_REDIR_TEMP:      // successful fetch, redirected
        case CrawlDatumHbase.STATUS_REDIR_PERM:
        case CrawlDatumHbase.STATUS_NOTMODIFIED:     // successful fetch, notmodified
          int modified = FetchSchedule.STATUS_UNKNOWN;
          if (status == CrawlDatumHbase.STATUS_NOTMODIFIED) {
            modified = FetchSchedule.STATUS_NOTMODIFIED;
          }
          byte[] prevSig = row.getPrevSignature();
          byte[] signature = row.getSignature();
          if (prevSig != null && signature != null) {
            if (SignatureComparator._compare(prevSig, signature) != 0) {
              modified = FetchSchedule.STATUS_MODIFIED;
            } else {
              modified = FetchSchedule.STATUS_NOTMODIFIED;
            }
          }
          long fetchTime = row.getFetchTime();
          long prevFetchTime = row.getPrevFetchTime();
          long modifiedTime = row.getModifiedTime();

          schedule.setFetchSchedule(url, row, prevFetchTime, 0L,
              fetchTime, modifiedTime, modified);
          if (maxInterval < row.getFetchInterval())
            schedule.forceRefetch(url, row, false);
          break;
        case CrawlDatumHbase.STATUS_RETRY:
          schedule.setPageRetrySchedule(url, row, 0L, 0L, row.getFetchTime());
          if (row.getRetriesSinceFetch() < retryMax) {
            row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
          } else {
            row.setStatus(CrawlDatumHbase.STATUS_GONE);
          }
          break;
        case CrawlDatumHbase.STATUS_GONE:
          schedule.setPageGoneSchedule(url, row, 0L, 0L, row.getFetchTime());
          break;
        }
      }
    }

    row.deleteAllInlinks();
    for (Inlink inlink : inlinks) {
       row.addInlink(inlink);
    }
    
    // clear markers
    row.deleteMeta(FetcherHbase.REDIRECT_DISCOVERED);
	row.deleteMeta(GeneratorHbase.TMP_FETCH_MARK);
    row.deleteMeta(FetcherHbase.TMP_PARSE_MARK);
	row.deleteMeta(ParseTable.TMP_UPDATE_MARK);
    
    output.collect(key, row.makeBatchUpdate());
  }
  
  private void updateTable(String table, boolean updateAll) throws IOException {
    LOG.info("UpdateTable: starting");
    LOG.info("UpdateTable: table: " + table);
	if (updateAll && LOG.isWarnEnabled())
	  LOG.warn("Running update with reset enabled - whole table will be modified.");
    JobConf job = new NutchJob(getConf());
	job.setBoolean(ALL, updateAll);
    job.setJobName("update-table " + table);
    TableMapReduce.initJob(table, TableUtil.getColumns(COLUMNS), 
                           UpdateTable.class, ImmutableBytesWritable.class, 
                           NutchWritable.class, job);
    
    JobClient.runJob(job);
    
    LOG.info("UpdateTable: done");
  }
  
  public int run(String[] args) throws Exception {
    String usage = "Usage: UpdateTable <webtable> [-all]";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }
	boolean updateAll = false;
	
	for (int i = 1; i < args.length; i++) {
	  if ("-all".equals(args[i]))
		updateAll = true;
	} 
    updateTable(args[0], updateAll);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new UpdateTable(), args);
    System.exit(res);
  }

}

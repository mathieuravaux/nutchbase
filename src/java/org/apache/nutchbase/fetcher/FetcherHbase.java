package org.apache.nutchbase.fetcher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.URLUtil;
import org.apache.nutchbase.crawl.CrawlDatumHbase;
import org.apache.nutchbase.crawl.GeneratorHbase;
import org.apache.nutchbase.protocol.ProtocolFactoryHbase;
import org.apache.nutchbase.protocol.ProtocolHbase;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.nutchbase.util.hbase.TableColumns;
import org.apache.nutchbase.util.hbase.TableUtil;

public class FetcherHbase extends Configured
implements MapRunnable<ImmutableBytesWritable, RowResult, ImmutableBytesWritable, RowPart>,
           Reducer<ImmutableBytesWritable, RowPart, ImmutableBytesWritable, BatchUpdate>,
           Tool {

  public static final Log LOG = LogFactory.getLog(FetcherHbase.class);

  public static final String REDIRECT_DISCOVERED = "__tmp_rdr_disc__";

  public static final String TMP_PARSE_MARK = "__tmp_parse_mark__";

  private static final Set<String> COLUMNS = new HashSet<String>();

  static {
    COLUMNS.add(TableColumns.FETCH_TIME_STR);
    COLUMNS.add(TableColumns.REPR_URL_STR);
    COLUMNS.add(TableColumns.METADATA_STR + GeneratorHbase.TMP_FETCH_MARK);
  }

  private OutputCollector<ImmutableBytesWritable, RowPart> output;
  private Reporter reporter;

  private final AtomicInteger activeThreads = new AtomicInteger(0);
  private final AtomicInteger spinWaiting = new AtomicInteger(0);

  private final long start = System.currentTimeMillis(); // start time of fetcher run
  private final AtomicLong lastRequestStart = new AtomicLong(start);

  private final AtomicLong bytes = new AtomicLong(0);        // total bytes fetched
  private final AtomicInteger pages = new AtomicInteger(0);  // total pages fetched
  private final AtomicInteger errors = new AtomicInteger(0); // total pages errored

  private FetchItemQueues fetchQueues;
  private QueueFeeder feeder;

  /**
   * This class described the item to be fetched.
   */
  private static class FetchItem {
    ImmutableBytesWritable key;
    RowPart row;
    String queueID;
    String url;
    URL u;

    public FetchItem(ImmutableBytesWritable key, RowPart row,
                     String url, URL u, String queueID) {
      this.key = key;
      this.row = row;
      this.url = url;
      this.u = u;
      this.queueID = queueID;
    }

    /** Create an item. Queue id will be created based on <code>byIP</code>
     * argument, either as a protocol + hostname pair, or protocol + IP
     * address pair.
     */
    public static FetchItem create(ImmutableBytesWritable key, RowPart row,
                                   String url, boolean byIP) {
      String queueID;
      URL u = null;
      try {
        u = new URL(url);
      } catch (final Exception e) {
        LOG.warn("Cannot parse url: " + url, e);
        return null;
      }
      final String proto = u.getProtocol().toLowerCase();
      String host;
      if (byIP) {
        try {
          final InetAddress addr = InetAddress.getByName(u.getHost());
          host = addr.getHostAddress();
        } catch (final UnknownHostException e) {
          // unable to resolve it, so don't fall back to host name
          LOG.warn("Unable to resolve: " + u.getHost() + ", skipping.");
          return null;
        }
      } else {
        host = u.getHost();
        if (host == null) {
          LOG.warn("Unknown host for url: " + url + ", skipping.");
          return null;
        }
        host = host.toLowerCase();
      }
      queueID = proto + "://" + host;
      return new FetchItem(key, row, url, u, queueID);
    }

  }

  /**
   * This class handles FetchItems which come from the same host ID (be it
   * a proto/hostname or proto/IP pair). It also keeps track of requests in
   * progress and elapsed time between requests.
   */
  private static class FetchItemQueue {
    List<FetchItem> queue = Collections.synchronizedList(new LinkedList<FetchItem>());
    Set<FetchItem>  inProgress = Collections.synchronizedSet(new HashSet<FetchItem>());
    AtomicLong nextFetchTime = new AtomicLong();
    long crawlDelay;
    long minCrawlDelay;
    int maxThreads;
    Configuration conf;

    public FetchItemQueue(Configuration conf, int maxThreads, long crawlDelay, long minCrawlDelay) {
      this.conf = conf;
      this.maxThreads = maxThreads;
      this.crawlDelay = crawlDelay;
      this.minCrawlDelay = minCrawlDelay;
      // ready to start
      setEndTime(System.currentTimeMillis() - crawlDelay);
    }

    public int getQueueSize() {
      return queue.size();
    }

    public int getInProgressSize() {
      return inProgress.size();
    }

    public void finishFetchItem(FetchItem it, boolean asap) {
      if (it != null) {
        inProgress.remove(it);
        setEndTime(System.currentTimeMillis(), asap);
      }
    }

    public void addFetchItem(FetchItem it) {
      if (it == null) return;
      queue.add(it);
    }

    public void addInProgressFetchItem(FetchItem it) {
      if (it == null) return;
      inProgress.add(it);
    }

    public FetchItem getFetchItem() {
      if (inProgress.size() >= maxThreads) return null;
      final long now = System.currentTimeMillis();
      if (nextFetchTime.get() > now) return null;
      FetchItem it = null;
      if (queue.size() == 0) return null;
      try {
        it = queue.remove(0);
        inProgress.add(it);
      } catch (final Exception e) {
        LOG.error("Cannot remove FetchItem from queue or cannot add it to inProgress queue", e);
      }
      return it;
    }

    public synchronized void dump() {
      LOG.info("  maxThreads    = " + maxThreads);
      LOG.info("  inProgress    = " + inProgress.size());
      LOG.info("  crawlDelay    = " + crawlDelay);
      LOG.info("  minCrawlDelay = " + minCrawlDelay);
      LOG.info("  nextFetchTime = " + nextFetchTime.get());
      LOG.info("  now           = " + System.currentTimeMillis());
      for (int i = 0; i < queue.size(); i++) {
        final FetchItem it = queue.get(i);
        LOG.info("  " + i + ". " + it.url);
      }
    }

    private void setEndTime(long endTime) {
      setEndTime(endTime, false);
    }

    private void setEndTime(long endTime, boolean asap) {
      if (!asap)
        nextFetchTime.set(endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
      else
        nextFetchTime.set(endTime);
    }
  }

  /**
   * Convenience class - a collection of queues that keeps track of the total
   * number of items, and provides items eligible for fetching from any queue.
   */
  private static class FetchItemQueues {
    public static final String DEFAULT_ID = "default";
    Map<String, FetchItemQueue> queues = new HashMap<String, FetchItemQueue>();
    AtomicInteger totalSize = new AtomicInteger(0);
    int maxThreads;
    boolean byIP;
    long crawlDelay;
    long minCrawlDelay;
    Configuration conf;

    public FetchItemQueues(Configuration conf) {
      this.conf = conf;
      this.maxThreads = conf.getInt("fetcher.threads.per.host", 1);
      // backward-compatible default setting
      this.byIP = conf.getBoolean("fetcher.threads.per.host.by.ip", false);
      this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
      this.minCrawlDelay = (long) (conf.getFloat("fetcher.server.min.delay", 0.0f) * 1000);
    }

    public int getTotalSize() {
      return totalSize.get();
    }

    public int getQueueCount() {
      return queues.size();
    }

    public void addFetchItem(ImmutableBytesWritable key, RowPart row,
                             String url) {
      final FetchItem it = FetchItem.create(key, row, url, byIP);
      if (it != null) addFetchItem(it);
    }

    public void addFetchItem(FetchItem it) {
      final FetchItemQueue fiq = getFetchItemQueue(it.queueID);
      fiq.addFetchItem(it);
      totalSize.incrementAndGet();
    }

    public void finishFetchItem(FetchItem it) {
      finishFetchItem(it, false);
    }

    public void finishFetchItem(FetchItem it, boolean asap) {
      final FetchItemQueue fiq = queues.get(it.queueID);
      if (fiq == null) {
        LOG.warn("Attempting to finish item from unknown queue: " + it);
        return;
      }
      fiq.finishFetchItem(it, asap);
    }

    public synchronized FetchItemQueue getFetchItemQueue(String id) {
      FetchItemQueue fiq = queues.get(id);
      if (fiq == null) {
        // initialize queue
        fiq = new FetchItemQueue(conf, maxThreads, crawlDelay, minCrawlDelay);
        queues.put(id, fiq);
      }
      return fiq;
    }

    public synchronized FetchItem getFetchItem() {
      final Iterator<Map.Entry<String, FetchItemQueue>> it =
        queues.entrySet().iterator();
      while (it.hasNext()) {
        final FetchItemQueue fiq = it.next().getValue();
        // reap empty queues
        if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
          it.remove();
          continue;
        }
        final FetchItem fit = fiq.getFetchItem();
        if (fit != null) {
          totalSize.decrementAndGet();

          return fit;
        }
      }
      return null;
    }

    public synchronized void dump() {
      for (final String id : queues.keySet()) {
        final FetchItemQueue fiq = queues.get(id);
        if (fiq.getQueueSize() == 0) continue;
        LOG.info("* queue: " + id);
        fiq.dump();
      }
    }
  }

  /**
   * This class feeds the queues with input items, and re-fills them as
   * items are consumed by FetcherThread-s.
   */
  private static class QueueFeeder extends Thread {
    private final RecordReader<ImmutableBytesWritable, RowResult> reader;
    private final FetchItemQueues queues;
    private final int size;

    public QueueFeeder(RecordReader<ImmutableBytesWritable, RowResult> reader,
        FetchItemQueues queues, int size) {
      this.reader = reader;
      this.queues = queues;
      this.size = size;
      this.setDaemon(true);
      this.setName("QueueFeeder");
    }

    @Override
    public void run() {
      boolean hasMore = true;
      int cnt = 0;

      while (hasMore) {
        int feed = size - queues.getTotalSize();
        if (feed <= 0) {
          // queues are full - spin-wait until they have some free space
          try {
            LOG.info("-feeder : spin-waiting while queues are full (size=" + size + ", queues.getTotalSize()=" + queues.getTotalSize() );
            Thread.sleep(1000);
          } catch (final Exception e) {};
          continue;
        } else {
          LOG.info("-feeding " + feed + " input urls ...");
          while (feed > 0 && hasMore) {
            try {
              final ImmutableBytesWritable key = new ImmutableBytesWritable();
              final RowResult rowResult = new RowResult();
              hasMore = reader.next(key, rowResult);
              if (hasMore) {
                final RowPart row = new RowPart(rowResult);
                if (!row.hasMeta(GeneratorHbase.TMP_FETCH_MARK)) {
                  // not marked by generate for fetching
                  continue;
                }
                final String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
                queues.addFetchItem(key, row, url);
                cnt++;
                feed--;
              }
            } catch (final IOException e) {
              LOG.fatal("QueueFeeder error reading input, record " + cnt, e);
              return;
            }
          }
        }
      }
      LOG.info("QueueFeeder finished: total " + cnt + " records.");
    }
  }

  /**
   * This class picks items from queues and fetches the pages.
   */
  private class FetcherThread extends Thread {
    private final URLFilters urlFilters;
    private final URLNormalizers normalizers;
    private final ProtocolFactoryHbase protocolFactory;
    private final long maxCrawlDelay;
    private final boolean byIP;
    private final int maxRedirect;
    private String reprUrl;
    private boolean redirecting;
    private int redirectCount;

    public FetcherThread(Configuration conf, int num) {
      this.setDaemon(true);                       // don't hang JVM on exit
      this.setName("FetcherThread" + num);        // use an informative name
      this.urlFilters = new URLFilters(conf);
      this.protocolFactory = new ProtocolFactoryHbase(conf);
      this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
      this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
      // backward-compatible default setting
      this.byIP = conf.getBoolean("fetcher.threads.per.host.by.ip", true);
      this.maxRedirect = conf.getInt("http.redirect.max", 3);
    }

    @Override
    public void run() {
      activeThreads.incrementAndGet(); // count threads

      FetchItem fit = null;
      try {

        while (true) {
          fit = fetchQueues.getFetchItem();
          // LOG.info("getFetchItem : " + fit);
          if (fit == null) {
            if (feeder.isAlive() || fetchQueues.getTotalSize() > 0) {
              LOG.debug(getName() + " fetchQueues.getFetchItem() was null, spin-waiting ...");
              // spin-wait.
              spinWaiting.incrementAndGet();
              try {
                Thread.sleep(500);
              } catch (final Exception e) {}
                spinWaiting.decrementAndGet();
              continue;
            } else {
              // all done, finish this thread
              return;
            }
          }
          lastRequestStart.set(System.currentTimeMillis());
          if (!fit.row.hasColumn(TableColumns.REPR_URL)) {
            reprUrl = fit.url.toString();
          } else {
            reprUrl = fit.row.getReprUrl();
          }
          try {
            LOG.info("fetching " + fit.url);

            // fetch the page
            redirecting = false;
            redirectCount = 0;
            do {
              if (LOG.isDebugEnabled()) {
                LOG.debug("redirectCount=" + redirectCount);
              }
              redirecting = false;
              final ProtocolHbase protocol = this.protocolFactory.getProtocol(fit.url);
              final RobotRules rules = protocol.getRobotRules(fit.url, fit.row);
              if (!rules.isAllowed(fit.u)) {
                // unblock
                fetchQueues.finishFetchItem(fit, true);
                if (LOG.isDebugEnabled()) {
                  LOG.info("Denied by robots.txt: " + fit.url);
                }
                output(fit, null, ProtocolStatus.STATUS_ROBOTS_DENIED,
                       CrawlDatumHbase.STATUS_GONE);
                continue;
              }
              if (rules.getCrawlDelay() > 0) {
                if (rules.getCrawlDelay() > maxCrawlDelay) {
                  // unblock
                  fetchQueues.finishFetchItem(fit, true);
                  LOG.info("Crawl-Delay for " + fit.url + " too long (" + rules.getCrawlDelay() + "), skipping");
                  output(fit, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatumHbase.STATUS_GONE);
                  continue;
                } else {
                  final FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
                  fiq.crawlDelay = rules.getCrawlDelay();
                }
              }
              final ProtocolOutput output = protocol.getProtocolOutput(fit.url, fit.row);
              final ProtocolStatus status = output.getStatus();
              final Content content = output.getContent();
              // unblock queue
              fetchQueues.finishFetchItem(fit);

              switch(status.getCode()) {

              case ProtocolStatus.WOULDBLOCK:
                // retry ?
                fetchQueues.addFetchItem(fit);
                break;

              case ProtocolStatus.SUCCESS:        // got a page
                output(fit, content, status, CrawlDatumHbase.STATUS_FETCHED);
                updateStatus(content.getContent().length);
                break;

              case ProtocolStatus.MOVED:         // redirect
              case ProtocolStatus.TEMP_MOVED:
                byte code;
                boolean temp;
                if (status.getCode() == ProtocolStatus.MOVED) {
                  code = CrawlDatumHbase.STATUS_REDIR_PERM;
                  temp = false;
                } else {
                  code = CrawlDatumHbase.STATUS_REDIR_TEMP;
                  temp = true;
                }
                output(fit, content, status, code);
                final String newUrl = status.getMessage();
                handleRedirect(fit.url, newUrl, temp,  Fetcher.PROTOCOL_REDIR);
                redirecting = false;
                break;
              case ProtocolStatus.EXCEPTION:
                logError(fit.url, status.getMessage());
                /* FALLTHROUGH */
              case ProtocolStatus.RETRY:          // retry
              case ProtocolStatus.BLOCKED:
                output(fit, null, status, CrawlDatumHbase.STATUS_RETRY);
                break;

              case ProtocolStatus.GONE:           // gone
              case ProtocolStatus.NOTFOUND:
              case ProtocolStatus.ACCESS_DENIED:
              case ProtocolStatus.ROBOTS_DENIED:
                output(fit, null, status, CrawlDatumHbase.STATUS_GONE);
                break;

              case ProtocolStatus.NOTMODIFIED:
                output(fit, null, status, CrawlDatumHbase.STATUS_NOTMODIFIED);
                break;

              default:
                if (LOG.isWarnEnabled()) {
                  LOG.warn("Unknown ProtocolStatus: " + status.getCode());
                }
                output(fit, null, status, CrawlDatumHbase.STATUS_RETRY);
              }

              if (redirecting && redirectCount >= maxRedirect) {
                fetchQueues.finishFetchItem(fit);
                if (LOG.isInfoEnabled()) {
                  LOG.info(" - redirect count exceeded " + fit.url);
                }
                output(fit, null, ProtocolStatus.STATUS_REDIR_EXCEEDED, CrawlDatumHbase.STATUS_GONE);
              }

            } while (redirecting && (redirectCount < maxRedirect));

          } catch (final Throwable t) {                 // unexpected exception
            // unblock
            fetchQueues.finishFetchItem(fit);
            t.printStackTrace();
            logError(fit.url, t.toString());
            output(fit, null, ProtocolStatus.STATUS_FAILED, CrawlDatumHbase.STATUS_RETRY);
          }
        }

      } catch (final Throwable e) {
        if (LOG.isFatalEnabled()) {
          e.printStackTrace(LogUtil.getFatalStream(LOG));
          LOG.fatal("fetcher caught:"+e.toString());
        }
      } finally {
        if (fit != null) fetchQueues.finishFetchItem(fit);
        activeThreads.decrementAndGet(); // count threads
        LOG.info("-finishing thread " + getName() + ", activeThreads=" + activeThreads);
      }
    }

    private void handleRedirect(String url, String newUrl,
                                boolean temp, String redirType)
    throws URLFilterException, IOException {
      newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      newUrl = urlFilters.filter(newUrl);
      if (newUrl == null || newUrl.equals(url)) {
        return;
      }
      reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
      final String reversedUrl = TableUtil.reverseUrl(reprUrl);
      final ImmutableBytesWritable newKey =
        new ImmutableBytesWritable(reversedUrl.getBytes());
      final RowPart newRow = new RowPart(newKey.get());
      if (!reprUrl.equals(url)) {
        newRow.setReprUrl(reprUrl);
      }
      newRow.putMeta(REDIRECT_DISCOVERED, TableUtil.YES_VAL);
      output.collect(newKey, newRow);
      if (LOG.isDebugEnabled()) {
        LOG.debug(" - " + redirType + " redirect to " +
            reprUrl + " (fetching later)");
      }

    }

    private void logError(String url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      errors.incrementAndGet();
    }

    private void output(FetchItem fit, Content content,
                        ProtocolStatus pstatus, byte status) {
      try {
        fit.row.setStatus(status);
        final long prevFetchTime = fit.row.getFetchTime();
        fit.row.setPrevFetchTime(prevFetchTime);
        fit.row.setFetchTime(System.currentTimeMillis());
        if (pstatus != null) {
          fit.row.setProtocolStatus(pstatus);
        }

        if (content != null) {
          fit.row.setContent(content.getContent());
          fit.row.setContentType(content.getContentType());
          fit.row.setBaseUrl(content.getBaseUrl());
          if (status == CrawlDatumHbase.STATUS_FETCHED)
            fit.row.putMeta(TMP_PARSE_MARK, TableUtil.YES_VAL);
        }  
        output.collect(fit.key, fit.row);
      } catch (final IOException e) {
        e.printStackTrace(LogUtil.getFatalStream(LOG));
        LOG.fatal("fetcher caught:"+e.toString());
      }
    }

  }

  private void updateStatus(int bytesInPage) throws IOException {
    pages.incrementAndGet();
    bytes.addAndGet(bytesInPage);
  }

  private void reportStatus() throws IOException {
    String status;
    final long elapsed = (System.currentTimeMillis() - start)/1000;
    status = activeThreads + " threads, " +
      pages+" pages, "+errors+" errors, "
      + Math.round(((float)pages.get()*10)/elapsed)/10.0+" pages/s, "
      + Math.round(((((float)bytes.get())*8)/1024)/elapsed)+" kb/s, ";
    reporter.setStatus(status);
  }

  public void run(RecordReader<ImmutableBytesWritable, RowResult> input,
      OutputCollector<ImmutableBytesWritable, RowPart> output,
      Reporter reporter) throws IOException {
    this.output = output;
    this.reporter = reporter;
    this.fetchQueues = new FetchItemQueues(getConf());
    final int threadCount = getConf().getInt("fetcher.threads.fetch", 10);
    LOG.info("Fetcher: threads: " + threadCount);

    feeder = new QueueFeeder(input, fetchQueues, threadCount * 50);
    //feeder.setPriority((Thread.MAX_PRIORITY + Thread.NORM_PRIORITY) / 2);
    feeder.start();

    // set non-blocking & no-robots mode for HTTP protocol plugins.
    getConf().setBoolean(Protocol.CHECK_BLOCKING, false);
    getConf().setBoolean(Protocol.CHECK_ROBOTS, false);

    for (int i = 0; i < threadCount; i++) {       // spawn threads
      new FetcherThread(getConf(), i).start();
    }
 // select a timeout that avoids a task timeout
    final long timeout = getConf().getInt("mapred.task.timeout", 10*60*1000)/10;

    do {                                          // wait for threads to exit
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {}

      reportStatus();
      LOG.info("-activeThreads=" + activeThreads + ", spinWaiting=" + spinWaiting.get()
          + ", fetchQueues= " + fetchQueues.getQueueCount() +", fetchQueues.totalSize=" + fetchQueues.getTotalSize());

      if (/* !feeder.isAlive() && */ fetchQueues.getTotalSize() < 20) {
          fetchQueues.dump();
      }
      // some requests seem to hang, despite all intentions
      if ((System.currentTimeMillis() - lastRequestStart.get()) > timeout) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Aborting with "+activeThreads+" hung threads.");
        }
        return;
      }

    } while (activeThreads.get() > 0);
    LOG.info("-activeThreads=" + activeThreads);

  }

  public void configure(JobConf job) {
  }

  public void close() throws IOException {
  }

  public void reduce(ImmutableBytesWritable key, Iterator<RowPart> values,
      OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
      Reporter reporter) throws IOException {
    while (values.hasNext()) {
      final RowPart row = values.next();
	  // remove the fetch-mark  
	  row.deleteMeta(GeneratorHbase.TMP_FETCH_MARK);
      output.collect(key, row.makeBatchUpdate());
    }
  }

  public void fetch(String table, int threads)
  throws IOException {

    LOG.info("FetcherHbase: starting");
    LOG.info("FetcherHbase: table: " + table);

    final JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + table);

    if (threads > 0) {
      job.setInt("fetcher.threads.fetch", threads);
    }

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    job.setInputFormat(TableInputFormat.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(RowPart.class);
    job.setMapRunnerClass(FetcherHbase.class);
    FileInputFormat.addInputPaths(job, table);
    job.set(TableInputFormat.COLUMN_LIST, getColumnsList(job));

    job.setOutputFormat(TableOutputFormat.class);
    job.setReducerClass(FetcherHbase.class);
    job.set(TableOutputFormat.OUTPUT_TABLE, table);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(BatchUpdate.class);

    JobClient.runJob(job);

    LOG.info("FetcherHbase: done");
  }

  private static final String getColumnsList(JobConf job) {
    final Set<String> columnSet = new HashSet<String>(COLUMNS);

    final ProtocolFactoryHbase protocolFactory = new ProtocolFactoryHbase(job);
    columnSet.addAll(protocolFactory.getColumnSet());

    return TableUtil.getColumns(columnSet);
  }

  public int run(String[] args) throws Exception {
    final String usage = "Usage: FetcherHbase <webtable> [-threads n]";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    final String table = args[0];

    int threads = -1;

    if (args.length == 3 && args[1].equals("-threads")) {
      // found -threads option
      threads =  Integer.parseInt(args[2]);
    }

    fetch(table, threads);              // run the Fetcher

    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(), new FetcherHbase(), args);
    System.exit(res);
  }
}

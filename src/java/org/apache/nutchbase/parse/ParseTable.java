package org.apache.nutchbase.parse;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.URLUtil;
import org.apache.nutchbase.crawl.CrawlDatumHbase;
import org.apache.nutchbase.crawl.SignatureFactoryHbase;
import org.apache.nutchbase.crawl.SignatureHbase;
import org.apache.nutchbase.fetcher.FetcherHbase;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.nutchbase.util.hbase.TableColumns;
import org.apache.nutchbase.util.hbase.TableMapReduce;
import org.apache.nutchbase.util.hbase.TableUtil;

public class ParseTable
extends TableMapReduce<ImmutableBytesWritable, RowPart>
implements Tool {

  public static final Log LOG = LogFactory.getLog(Parser.class);

  public static final String TMP_UPDATE_MARK = "__tmp_update_mark__";

  private static final Set<String> COLUMNS = new HashSet<String>();

  static {
    COLUMNS.add(TableColumns.STATUS_STR);
    COLUMNS.add(TableColumns.CONTENT_STR);
    COLUMNS.add(TableColumns.CONTENT_TYPE_STR);
    COLUMNS.add(TableColumns.METADATA_STR + FetcherHbase.TMP_PARSE_MARK);
  }

  private ParseUtilHbase parseUtil;
  private SignatureHbase sig;
  private URLFilters filters;
  private URLNormalizers normalizers;
  private int maxOutlinks;
  private boolean ignoreExternalLinks;


  @Override
  public void configure(JobConf job) {
    super.configure(job);
    parseUtil = new ParseUtilHbase(job);
    sig = SignatureFactoryHbase.getSignature(job);
    filters = new URLFilters(job);
    normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_OUTLINK);
    int maxOutlinksPerPage = job.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
                                           : maxOutlinksPerPage;
    ignoreExternalLinks = job.getBoolean("db.ignore.external.links", false);

  }

  @Override
  public void map(ImmutableBytesWritable key, RowResult rowResult,
      OutputCollector<ImmutableBytesWritable, RowPart> output,
      Reporter reporter)
  throws IOException {
    RowPart row = new RowPart(rowResult);
    String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));

    if (!row.hasMeta(FetcherHbase.TMP_PARSE_MARK)) {
      return;
    }

    byte status = row.getStatus();
    if (status != CrawlDatumHbase.STATUS_FETCHED) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + url + " as status: " + 
            CrawlDatumHbase.getName(status));
      }
      return;
    }

    ParseHbase parse;
    try {
      parse = parseUtil.parse(url, row);
    } catch (Exception e) {
      LOG.warn("Error parsing: " + key + ": " + StringUtils.stringifyException(e));
      return;
    }

    byte[] signature = sig.calculate(row, parse);

    ParseStatus pstatus = parse.getParseStatus();
    row.setParseStatus(pstatus);
    if (pstatus.isSuccess()) {
      if (pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
        String newUrl = pstatus.getMessage();
        int refreshTime = Integer.parseInt(pstatus.getArgs()[1]);
        newUrl = normalizers.normalize(newUrl,
                                       URLNormalizers.SCOPE_FETCHER);
        try {
          newUrl = filters.filter(newUrl);
          if (newUrl == null || newUrl.equals(url)) {
            String reprUrl = URLUtil.chooseRepr(url, newUrl,
                                 refreshTime < Fetcher.PERM_REFRESH_TIME);
            String reversedUrl = TableUtil.reverseUrl(reprUrl);
            ImmutableBytesWritable newKey =
              new ImmutableBytesWritable(reversedUrl.getBytes());
            RowPart newRow = new RowPart(newKey.get());
            if (!reprUrl.equals(url)) {
              newRow.setReprUrl(reprUrl);
            }
            newRow.putMeta(FetcherHbase.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
            output.collect(newKey, newRow);
          }
        } catch (Exception e) {
          // ignore
        }
      } else {
        row.setText(parse.getText());
        row.setTitle(parse.getTitle());
        row.setSignature(signature);
        row.deleteAllOutlinks();
        Outlink[] outlinks = parse.getOutlinks();
        int count = 0;
        String fromHost;
        if (ignoreExternalLinks) {
          try {
            fromHost = new URL(url).getHost().toLowerCase();
          } catch (MalformedURLException e) {
            fromHost = null;
          }
        } else {
          fromHost = null;
        }
        for (int i = 0; count < maxOutlinks && i < outlinks.length; i++) {
          String toUrl = outlinks[i].getToUrl();
          toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
          try {
            toUrl = filters.filter(toUrl);
          } catch (URLFilterException e) {
            continue;
          }
          if (toUrl == null) {
            continue;
          }
          String toHost;
          if (ignoreExternalLinks) {
            try {
              toHost = new URL(toUrl).getHost().toLowerCase();
            } catch (MalformedURLException e) {
              toHost = null;
            }
            if (toHost == null || !toHost.equals(fromHost)) { // external links
              continue; // skip it
            }
          }

          row.addOutlink(new Outlink(toUrl, outlinks[i].getAnchor()));
        }
        row.putMeta(TMP_UPDATE_MARK, TableUtil.YES_VAL);
      }
    }

    output.collect(key, row);
  }

  @Override
  public void reduce(ImmutableBytesWritable key, Iterator<RowPart> values,
      OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
      Reporter reporter)
  throws IOException {
    output.collect(key, values.next().makeBatchUpdate(key.get()));
  }

  public void parse(String table) throws IOException {

    LOG.info("ParseHbase: starting");
    LOG.info("ParseHbase: segment: " + table);

    JobConf job = new NutchJob(getConf());
    job.setJobName("parse-hbase " + table);

    TableMapReduce.initJob(table, getColumns(job),
        ParseTable.class, ImmutableBytesWritable.class,
        RowPart.class, job);

    JobClient.runJob(job);
    LOG.info("ParseHbase: done");
  }

  private String getColumns(JobConf job) {
    Set<String> columnSet = new HashSet<String>(COLUMNS);
    ParserFactoryHbase parserFactory = new ParserFactoryHbase(job);
    columnSet.addAll(parserFactory.getColumnSet());
    columnSet.addAll(SignatureFactoryHbase.getSignature(job).getColumnSet());
    return TableUtil.getColumns(columnSet);
  }

  public int run(String[] args) throws Exception {
    final String usage = "Usage: ParseSegment <webtable>";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    parse(args[0]);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ParseTable(), args);
    System.exit(res);
  }

}

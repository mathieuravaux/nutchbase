package org.apache.nutchbase.indexer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexerOutputFormat;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.indexer.lucene.LuceneWriter;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.apache.nutchbase.util.hbase.ImmutableRowPart;
import org.apache.nutchbase.util.hbase.TableColumns;
import org.apache.nutchbase.util.hbase.TableUtil;

public class IndexerHbase 
extends MapReduceBase
implements Tool, TableMap<ImmutableBytesWritable, ImmutableRowPart>,
                  Reducer<ImmutableBytesWritable, ImmutableRowPart,
                          ImmutableBytesWritable, NutchDocument>{


  public static final Log LOG = LogFactory.getLog(IndexerHbase.class);
  
  private static final Set<String> COLUMNS = new HashSet<String>();
  
  private Configuration conf;
  
  private IndexingFiltersHbase filters;
    
  static {
    COLUMNS.add(TableColumns.SIGNATURE_STR);
    COLUMNS.add(TableColumns.PARSE_STATUS_STR);
    COLUMNS.add(TableColumns.SCORE_STR);
  }
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  public void configure(JobConf job) {
    filters = new IndexingFiltersHbase(job);
  }
  
  public void map(ImmutableBytesWritable key, RowResult rowResult,
      OutputCollector<ImmutableBytesWritable, ImmutableRowPart> output, 
      Reporter reporter)
  throws IOException {
    ImmutableRowPart row = new ImmutableRowPart(rowResult);
    
    if (!row.hasColumn(TableColumns.PARSE_STATUS)) {
      return;
    }
    
    ParseStatus pstatus = row.getParseStatus();
    if (!pstatus.isSuccess() || 
        pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
      return;   // filter urls not parsed
    }
    
    output.collect(key, row);
  }
  
  public void reduce(ImmutableBytesWritable key,
      Iterator<ImmutableRowPart> values,
      OutputCollector<ImmutableBytesWritable, NutchDocument> output,
      Reporter reporter) throws IOException {

    ImmutableRowPart row = values.next();
    NutchDocument doc = new NutchDocument();
    
    doc.add("digest", StringUtil.toHexString(row.getSignature()));
    
    String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
    try {
      doc = filters.filter(doc, url, row);
    } catch (IndexingException e) {
      LOG.warn("Error indexing "+key+": "+e);
      return;
    }
    
    // skip documents discarded by indexing filters
    if (doc == null) return;
    
    float boost = row.getScore();
    
    doc.setScore(boost);
    // store boost for use by explain and dedup
    doc.add("boost", Float.toString(boost));
    
    output.collect(key, doc);
  }
  
  private Set<String> getColumnSet(JobConf job) {
    Set<String> columnSet = new HashSet<String>(COLUMNS);
    IndexingFiltersHbase filters = new IndexingFiltersHbase(job);
    columnSet.addAll(filters.getColumnSet());
    return columnSet;  
  }
  
  private void index(Path indexDir, String table) throws IOException {
    LOG.info("IndexerHbase: starting");
    LOG.info("IndexerHbase: table: " + table);
    
    JobConf job = new NutchJob(getConf());
    job.setJobName("index " + table);
    TableMapReduceUtil.initTableMapJob(table,
        TableUtil.getColumns(getColumnSet(job)),
        IndexerHbase.class, ImmutableBytesWritable.class,
        ImmutableRowPart.class, job);

    job.setReducerClass(IndexerHbase.class);
    FileOutputFormat.setOutputPath(job, indexDir);
    job.setOutputFormat(IndexerOutputFormat.class);
    
    LuceneWriter.addFieldOptions("segment", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, job);
    LuceneWriter.addFieldOptions("digest", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, job);
    LuceneWriter.addFieldOptions("boost", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, job);
    
    NutchIndexWriterFactory.addClassToConf(job, LuceneWriter.class);
    
    JobClient.runJob(job);
    
    LOG.info("IndexerHbase: done");
  }
  
  public int run(String[] args) throws Exception {
    String usage = "Usage: IndexerHbase <index> <webtable>";

    if (args.length != 2) {
      System.err.println(usage);
      System.exit(-1);
    }

    index(new Path(args[0]), args[1]);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new IndexerHbase(), args);
    System.exit(res);
  }
}

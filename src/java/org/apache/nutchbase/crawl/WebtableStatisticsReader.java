package org.apache.nutchbase.crawl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.nutchbase.util.hbase.TableColumns;
import org.apache.nutchbase.util.hbase.TableMapReduce;
import org.apache.nutchbase.util.hbase.TableUtil;

public class WebtableStatisticsReader extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(WebtableStatisticsReader.class);
  

  private static final Set<String> COLUMNS = new HashSet<String>();
  
  static {
    COLUMNS.add(TableColumns.FETCH_TIME_STR);
    COLUMNS.add(TableColumns.SCORE_STR);
    COLUMNS.add(TableColumns.STATUS_STR);
  }

  
  
  public static class CrawlDbStatMapReduce
  extends TableMapReduce<ImmutableBytesWritable, LongWritable>
//  implements Mapper<Text, CrawlDatumHbase, Text, LongWritable>
  {
    LongWritable COUNT_1 = new LongWritable(1);
    private boolean sort = false;
    public void configure(JobConf job) {
      sort = job.getBoolean("db.reader.stats.sort", false );
    }
    public void close() {}
    public void map(Text key, CrawlDatumHbase value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
//      output.collect(new Text("T"), COUNT_1);
//      output.collect(new Text("status " + value.getName()), COUNT_1);
//      output.collect(new Text("retry " + value.getRetriesSinceFetch()), COUNT_1);
//      output.collect(new Text("s"), new LongWritable((long) (value.getScore() * 1000.0)));
//      if(sort){
//        URL u = new URL(key.toString());
//        String host = u.getHost();
//        output.collect(new Text("status " + value.getName()() + " " + host), COUNT_1);
//      }
    }

    @Override
	public void map(ImmutableBytesWritable key, RowResult rowResult,
			OutputCollector<ImmutableBytesWritable, LongWritable> output,
			Reporter reporter) throws IOException {
		
        String reversedUrl = Bytes.toString(key.get());
        String url = TableUtil.unreverseUrl(reversedUrl);
        
        RowPart row = new RowPart(rowResult);
//        System.out.println("url :" + url + " / " +  row.getFetchTime());
        
        output.collect(new ImmutableBytesWritable("T".getBytes()), COUNT_1);
        String status = "status " + CrawlDatumHbase.getName(row.getStatus());
        output.collect(new ImmutableBytesWritable(status.getBytes()), COUNT_1);
//      	output.collect(new Text("retry " + row.getRetriesSinceFetch()), COUNT_1);
        output.collect(new ImmutableBytesWritable("s".getBytes()), new LongWritable((long) (row.getScore() * 1000.0)));


		
	}
	@Override
	public void reduce(ImmutableBytesWritable key, Iterator<LongWritable> values,
			OutputCollector<ImmutableBytesWritable, BatchUpdate> output,
			Reporter reporter) throws IOException {
		System.out.println("Reducer::reduce");
	}
  }
  
//  public static class CrawlDbStatCombiner implements Reducer<Text, LongWritable, Text, LongWritable> {
//    LongWritable val = new LongWritable();
//    
//    public CrawlDbStatCombiner() { }
//    public void configure(JobConf job) { }
//    public void close() {}
//    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
//        throws IOException {
//      val.set(0L);
//      String k = ((Text)key).toString();
//      if (!k.equals("s")) {
//        while (values.hasNext()) {
//          LongWritable cnt = (LongWritable)values.next();
//          val.set(val.get() + cnt.get());
//        }
//        output.collect(key, val);
//      } else {
//        long total = 0;
//        long min = Long.MAX_VALUE;
//        long max = Long.MIN_VALUE;
//        while (values.hasNext()) {
//          LongWritable cnt = (LongWritable)values.next();
//          if (cnt.get() < min) min = cnt.get();
//          if (cnt.get() > max) max = cnt.get();
//          total += cnt.get();
//        }
//        output.collect(new Text("scn"), new LongWritable(min));
//        output.collect(new Text("scx"), new LongWritable(max));
//        output.collect(new Text("sct"), new LongWritable(total));
//      }
//    }
//  }

  public static class CrawlDbStatReducer implements Reducer<ImmutableBytesWritable, LongWritable, Text, LongWritable> {
    public void configure(JobConf job) {}
    public void close() {}
    public void reduce(ImmutableBytesWritable key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {

      String k = Bytes.toString(((ImmutableBytesWritable) key).get());
//      System.out.println("k :" + k);
      if (k.equals("T")) {
        // sum all values for this key
        long sum = 0;
        while (values.hasNext()) {
          sum += ((LongWritable) values.next()).get();
        }
        // output sum
//        output.collect(new Text(k), new LongWritable(sum));
        System.out.println(k + "  --> " + sum);
      } else if (k.startsWith("status") || k.startsWith("retry")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          cnt.set(cnt.get() + val.get());
        }
//        output.collect(new Text(k), cnt);
        System.out.println(k + "  --> " + cnt);
      } else if (k.equals("scx")) {
        LongWritable cnt = new LongWritable(Long.MIN_VALUE);
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          if (cnt.get() < val.get()) cnt.set(val.get());
        }
//        output.collect(new Text(k), cnt);
        System.out.println(k + "  --> " + cnt);
      } else if (k.equals("scn")) {
        LongWritable cnt = new LongWritable(Long.MAX_VALUE);
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          if (cnt.get() > val.get()) cnt.set(val.get());
        }
//        output.collect(new Text(k), cnt);
        System.out.println(k + "  --> " + cnt);
      } else if (k.equals("sct")) {
        LongWritable cnt = new LongWritable();
        while (values.hasNext()) {
          LongWritable val = (LongWritable)values.next();
          cnt.set(cnt.get() + val.get());
        }
//        output.collect(new Text(k), cnt);
        System.out.println(k + "  --> " + cnt);
      }
    }
  }


  
  
  public void processStatJob(String webtable, boolean sort) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Webtable statistics start: " + webtable);
    }
    
    Path tmpFolder = new Path("/tmp", "stat_tmp" + System.currentTimeMillis());
	JobConf job = new NutchJob(getConf());
	job.setJobName("stats " + webtable);
	job.setBoolean("db.reader.stats.sort", sort);

    TableMapReduce.initJob(webtable, TableUtil.getColumns(COLUMNS), 
    		CrawlDbStatMapReduce.class,
    		ImmutableBytesWritable.class,
            LongWritable.class,
            job);

//    job.setCombinerClass(CrawlDbStatCombiner.class);
    job.setReducerClass(CrawlDbStatReducer.class);
    FileOutputFormat.setOutputPath(job, tmpFolder);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobClient.runJob(job);

    
    
    
//    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
//    job.setInputFormat(SequenceFileInputFormat.class);
//    job.setMapperClass(CrawlDbStatMapper.class);
//    job.setCombinerClass(CrawlDbStatCombiner.class);
//    job.setReducerClass(CrawlDbStatReducer.class);
//    FileOutputFormat.setOutputPath(job, tmpFolder);
//    job.setOutputFormat(SequenceFileOutputFormat.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(LongWritable.class);


    // reading the result
//    FileSystem fileSystem = FileSystem.get(getConf());
//    SequenceFile.Reader[] readers = SequenceFileOutputFormat.getReaders(getConf(), tmpFolder);
//
//    Text key = new Text();
//    LongWritable value = new LongWritable();
//
//    TreeMap<String, LongWritable> stats = new TreeMap<String, LongWritable>();
//    for (int i = 0; i < readers.length; i++) {
//      SequenceFile.Reader reader = readers[i];
//      while (reader.next(key, value)) {
//        String k = key.toString();
//        LongWritable val = stats.get(k);
//        if (val == null) {
//          val = new LongWritable();
//          if (k.equals("scx")) val.set(Long.MIN_VALUE);
//          if (k.equals("scn")) val.set(Long.MAX_VALUE);
//          stats.put(k, val);
//        }
//        if (k.equals("scx")) {
//          if (val.get() < value.get()) val.set(value.get());
//        } else if (k.equals("scn")) {
//          if (val.get() > value.get()) val.set(value.get());          
//        } else {
//          val.set(val.get() + value.get());
//        }
//      }
//      reader.close();
//    }
//    
//    if (LOG.isInfoEnabled()) {
//      LOG.info("Statistics for WebTable: " + webtable);
//      LongWritable totalCnt = stats.get("T");
//      stats.remove("T");
//      LOG.info("TOTAL urls:\t" + totalCnt.get());
//      for (Map.Entry<String, LongWritable> entry : stats.entrySet()) {
//        String k = entry.getKey();
//        LongWritable val = entry.getValue();
//        if (k.equals("scn")) {
//          LOG.info("min score:\t" + (float) (val.get() / 1000.0f));
//        } else if (k.equals("scx")) {
//          LOG.info("max score:\t" + (float) (val.get() / 1000.0f));
//        } else if (k.equals("sct")) {
//          LOG.info("avg score:\t" + (float) ((((double)val.get()) / totalCnt.get()) / 1000.0));
//        } else if (k.startsWith("status")) {
//          String[] st = k.split(" ");
//          int code = Integer.parseInt(st[1]);
//          if(st.length >2 ) LOG.info("   " + st[2] +" :\t" + val);
//          else LOG.info(st[0] +" " +code + " (" + CrawlDatumHbase.getName((byte) code) + "):\t" + val);
//        } else LOG.info(k + ":\t" + val);
//      }
//    }
//    // removing the tmp folder
//    fileSystem.delete(tmpFolder, true);
    if (LOG.isInfoEnabled()) { LOG.info("CrawlDb statistics: done"); }

  }
  

  public void readUrl(String webtable, String url) throws IOException {
	  Text key = new Text(url);
	  //CrawlDatum val = new CrawlDatum();
	  //CrawlDatum res = (CrawlDatum)MapFileOutputFormat.getEntry(readers, new HashPartitioner<Text, CrawlDatum>(), key, val);
	  // System.out.println("URL: " + url);
	  // if (res != null) {
      // 	System.out.println(res);
      // } else {
      // 	System.out.println("not found");
      // }
  }
  
  

  
  public int run(String[] args) throws Exception {
	    if (args.length < 1) {
	      System.err.println("Usage: CrawlDbReader <webtable> -stats | -url <url>)");
	      System.err.println("\t<crawldb>\tdirectory name where crawldb is located");
	      System.err.println("\t-stats [-sort] \tprint overall statistics to System.out");
	      System.err.println("\t\t[-sort]\tlist status sorted by host");
	      System.err.println("\t-url <url>\tprint information on <url> to System.out");
	      System.err.println("\t\t[<min>]\tskip records with scores below this value.");
	      System.err.println("\t\t\tThis can significantly improve performance.");
	      return -1;
	    }
	    String table = args[0];

	    for (int i = 1; i < args.length; i++) {
	      if (args[i].equals("-stats")) {
	        boolean toSort = false;
	        if(i < args.length - 1 && "-sort".equals(args[i+1])){
	          toSort = true;
	          i++;
	        }
	        processStatJob(table, toSort);
	      } else if (args[i].equals("-url")) {
	        readUrl(table, args[++i]);
	      } else {
	        System.err.println("\nError: wrong argument " + args[i]);
	      }
	    }
	    return 0;

	  }

  
  public static void main(String args[]) throws Exception {
	int res = ToolRunner.run(NutchConfiguration.create(), new WebtableStatisticsReader(), args);
	System.exit(res);
  }

  
}

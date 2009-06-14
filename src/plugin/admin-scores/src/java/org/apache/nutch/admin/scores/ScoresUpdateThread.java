package org.apache.nutch.admin.scores;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.nutch.admin.TaskThread;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.CrawlDbFilter;
import org.apache.nutch.crawl.CrawlDbMerger;
import org.apache.nutch.crawl.CrawlDbReducer;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.CrawlDbMerger.Merger;
import org.apache.nutch.indexer.DeleteDuplicates;
import org.apache.nutch.indexer.IndexMerger;
import org.apache.nutch.indexer.Indexer;
import org.apache.nutch.net.URLFilter;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.segment.SegmentMerger;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import java.util.ArrayList;









class CrawlDbMetasMerger extends MapReduceBase implements  Mapper<Text, CrawlDatum, Text, CrawlDatum> {
	private static final Logger LOG = Logger.getLogger(CrawlDbMetasMerger.class.getName());

	public void close() throws IOException {}
	public void configure(JobConf conf) {}
		
	Map<String, List<Modification>> modificationsMap;
	
	public CrawlDbMetasMerger() {
		LOG.warn("CrawlDbMetasMerger instanciated.");
    	Path modifs_filename = new Path("modifications.txt");
    	FileInputStream fis = null;
    	ObjectInputStream in = null;
    	
    	try {
			fis = new FileInputStream(modifs_filename.toString());
			in = new ObjectInputStream(fis);
			modificationsMap = (Map)in.readObject();
			in.close();
			fis.close();
		} catch (Exception e) {
			LOG.fatal("Opening the modifications map file was impossible." +
					org.apache.hadoop.util.StringUtils.stringifyException(e) );
		}

	}

	public void init() { init(NutchConfiguration.create()); }
	public void init(Configuration conf) {}
	
	private org.apache.hadoop.io.MapWritable meta = new org.apache.hadoop.io.MapWritable();
	private CrawlDatum res = new CrawlDatum();
	
	public void map(Text key, CrawlDatum value, OutputCollector<Text, CrawlDatum> output, Reporter reporter) throws IOException {
		meta.clear();
		meta = value.getMetaData();
		
		String url = key.toString();
		if(modificationsMap.containsKey(url)) {
			LOG.warn("Modification in metas found for url : " + url);
			List<Modification> modifs = modificationsMap.get(url);
			for (Modification m : modifs) {
				LOG.info("Modifiying " + m.meta + " -> " + m.newValue);
				meta.put(new Text(m.meta), new FloatWritable(m.newValue));
			}
		}
		
		res.setMetaData(meta);
		output.collect(key, res);
	}
}


public class ScoresUpdateThread  extends TaskThread {
	private static final Logger LOG = Logger.getLogger(ScoresUpdateThread.class.getName());

	Path crawlDir;
	Map<String, List<Modification>> modificationsMap;
	


		
	public ScoresUpdateThread(Configuration configuration, Path _instanceFolder, Map<String, List<Modification>> modifs) {
		super(configuration);
		crawlDir = new Path(configuration.get("crawl.dir"));
		modificationsMap = modifs;
	}

	private String log(String msg) {
		this.fMessage = msg;
		LOG.info(msg);
		return msg;
	}
	
	private String fatal(String msg) {
		this.fMessage = "FATAL ERROR : " + msg;
		LOG.fatal(this.fMessage);
		return msg;
	}
	
	public void run(){
		Path crawlDb = new Path(crawlDir, "crawldb");
		Path linkDb = new Path(crawlDir + "/linkdb");
	    Path segmentsDir = new Path(crawlDir, "segments");
		Path indexesDir = new Path(crawlDir + "/indexes");
	    Path indexDir = new Path(crawlDir + "/index");

		CrawlDb crawlDbTool = new CrawlDb(this.fConfiguration);
		
		FileSystem fs = null;
	    try {
	    	fs = FileSystem.get(this.fConfiguration);

	    	// Update Metas into CrawlDb
	    	Path modifs_filename = new Path("modifications.txt");
	    	FileOutputStream fos = null;
	    	ObjectOutputStream out = null;
	    	
	    	for (String url : modificationsMap.keySet()) {
	    		System.out.println(url);
				for(Modification m : modificationsMap.get(url)) {
					System.out.println(m.meta + "- " + m.newValue);
				}
			}
	    	fos = new FileOutputStream(modifs_filename.toString());
	    	out = new ObjectOutputStream(fos);
	    	out.writeObject(modificationsMap);
	    	out.close();
	    	fos.close();
	    	
	    	
	    	
	        Path workDir = new Path(this.fConfiguration.get("hadoop.tmp.dir"));
	        Path newCrawlDb = new Path(workDir, "crawldb-merge-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
	        log("newCrawlDb  path : \n" + newCrawlDb );
	        
	    	JobConf job = new NutchJob(this.fConfiguration);
			job.setJobName("crawldb MetaData merge " + newCrawlDb);
			
			job.setInputFormat(SequenceFileInputFormat.class);
			job.setBoolean(CrawlDbFilter.URL_FILTERING, false);
			job.setBoolean(CrawlDbFilter.URL_NORMALIZING, false);
			
			job.set("modifications_filename", modifs_filename.toString());
			
			job.setMapperClass(CrawlDbMetasMerger.class);
			job.setReducerClass(CrawlDbReducer.class);
			

			FileOutputFormat.setOutputPath(job, newCrawlDb);
			job.setOutputFormat(MapFileOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(CrawlDatum.class);
			
			FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
			
			Path lock = new Path(crawlDb, CrawlDb.LOCK_NAME);
		    LockUtil.createLockFile(fs, lock, true);
		    
		    try {
		    	JobClient.runJob(job);
		    } catch (IOException e) {
		    	LockUtil.removeLockFile(fs, lock);
		        //Path outPath = FileOutputFormat.getOutputPath(job);
		        //if (fs.exists(outPath) ) fs.delete(outPath, true);
		    	LOG.info("Job error : " + org.apache.hadoop.util.StringUtils.stringifyException(e));
		        throw e;
		    }
			
	    	// install new crawldb
	        log("Installing new CrawlDb...");
		    newCrawlDb = FileOutputFormat.getOutputPath(job);
		    Path old = new Path(crawlDb, "old");
		    Path current = new Path(crawlDb, CrawlDb.CURRENT_NAME);
		    if (fs.exists(current)) {
		      if (fs.exists(old)) fs.delete(old, true);
		      fs.rename(current, old);
		    }
		    fs.mkdirs(crawlDb);
		    fs.rename(newCrawlDb, current);
		    if (fs.exists(old)) { fs.delete(old, true); }
		    LockUtil.removeLockFile(fs, lock);
	    	
	    	
	    	// Reindex
	        FileStatus[] fstats = fs.listStatus(segmentsDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
	        Path[] segments = HadoopFSUtil.getPaths(fstats);
	        log("Segment list : \n" + Arrays.toString(segments));

	        log("Reindexing...");
	        if(indexesDir != null) {
	        	if (fs.exists(indexesDir)) { fs.delete(indexesDir, true); }
	            if (fs.exists(indexDir)) { fs.delete(indexDir, true); }
	        }
	        
	        Indexer indexer = new Indexer(this.fConfiguration);
	        // Arrays.asList(HadoopFSUtil.getPaths(fs.listStatus(segments, HadoopFSUtil.getPassDirectoriesFilter(fs))))
	        indexer.index(indexesDir, crawlDb, linkDb, Arrays.asList(segments));
	        if(indexesDir != null) {
	        	DeleteDuplicates dedup = new DeleteDuplicates(this.fConfiguration);
		        dedup.dedup(new Path[] { indexesDir });
		        
		        log("Merging indexes...");
		        IndexMerger merger = new IndexMerger(this.fConfiguration);
		        //Path workDir = new Path(crawlDir, "merger-output-" + System.currentTimeMillis());
		        
		        Path mergeDir = new Path(workDir, "indexmerger-" + System.currentTimeMillis());
		        Path outputIndex = indexDir;
		        List<Path> indexDirs = new ArrayList<Path>();
		        fstats = fs.listStatus(indexesDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
		        indexDirs.addAll(Arrays.asList(HadoopFSUtil.getPaths(fstats)));
		        Path[] indexFiles = (Path[])indexDirs.toArray(new Path[indexDirs.size()]);
		        //merger.merge(HadoopFSUtil.getPaths(fs.listStatus(indexesDir, HadoopFSUtil.getPassDirectoriesFilter(fs))), indexDir, workDir);
		        merger.merge(indexFiles, outputIndex, mergeDir);
		        fs.delete(mergeDir, true);
	        }
	        log("index done.");
	    	
	        
	    } catch (IOException e) {
	    	fatal("Update job error : \n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
	    }
	}
	
}



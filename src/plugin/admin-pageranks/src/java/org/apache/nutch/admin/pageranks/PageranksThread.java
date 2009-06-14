package org.apache.nutch.admin.pageranks;

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
import org.apache.hadoop.io.MapFile;
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
import org.apache.nutch.admin.GuiComponent;
import org.apache.nutch.admin.TaskThread;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.CrawlDbFilter;
import org.apache.nutch.crawl.CrawlDbMerger;
import org.apache.nutch.crawl.CrawlDbReader;
import org.apache.nutch.crawl.CrawlDbReducer;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.CrawlDbMerger.Merger;
import org.apache.nutch.indexer.DeleteDuplicates;
import org.apache.nutch.indexer.IndexMerger;
import org.apache.nutch.indexer.Indexer;
import org.apache.nutch.net.URLFilter;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.segment.SegmentMerger;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import java.util.ArrayList;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;


public class PageranksThread  extends TaskThread {
	private static final Logger LOG = Logger.getLogger(PageranksThread.class.getName());
	public static final String KEY = "Pageranks_thread"; 
	
	public static final int PAGERANK_POLL_DELAY = 800;
	
	Path crawlDir;
	Path crawlDb;
	boolean crawlDbOpened = false;
	MapFile.Reader[] readers;
	Map<String, Float> modificationsMap;
	Configuration configuration;

	private void open(boolean force) {
		if(!force && crawlDbOpened) { return; }
		LOG.info("Opening crawldb... in " + crawlDb);

		try {
			FileSystem fs = FileSystem.get(configuration);
			if (fs.exists(crawlDb)) {
				readers = MapFileOutputFormat.getReaders( fs, new Path(crawlDb, CrawlDb.CURRENT_NAME), configuration );
			}
		} catch (IOException e) {
			LOG.error("CrawlDb opening error : ");
			LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
		}
		LOG.info("CrawlDb opened.");
		crawlDbOpened = true;
	}


	public PageranksThread(Configuration configuration, Path _instanceFolder, Map<String, Float> modifs) {
		super(configuration);
		this.configuration = configuration;  
		crawlDir = new Path(configuration.get("crawl.dir"));
		crawlDb = new Path(crawlDir, "crawldb");
		modificationsMap = modifs;
	}

	private String log(String msg) {
		this.fMessage = msg;
		//LOG.info(msg);
		return msg;
	}
	
	private String fatal(String msg) {
		this.fMessage = "FATAL ERROR : " + msg;
		LOG.fatal(this.fMessage);
		return msg;
	}
	
	public void run(){
		log("Pagerank thread starting...");
		open(true);
		
		Text url = new Text();
		CrawlDatum datum = new CrawlDatum();
		
		for(MapFile.Reader r : Arrays.asList(readers)) {
			try {
				while(r.next(url, datum)) {
					log("url : " + url.toString());
				    try { Thread.sleep(PAGERANK_POLL_DELAY); } catch (Exception e) {
				    	fatal("Pagerank job error : \n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
				    }
				}
			} catch (IOException e) {
		    	fatal("Pagerank job error : \n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
			}
		}
		
		
	    log("Pagerank thread pass terminated. starting over...");
	    run();
	}

	
	public static class PageranksThreadConstructor implements ServletContextListener {
	    public void contextDestroyed(ServletContextEvent sce) { }
	    public void contextInitialized(ServletContextEvent sce) {
	    	LOG.info("Creating new pagerank fetcher thread...");
	    	
	    	final ServletContext app = sce.getServletContext();
	    	GuiComponent component = (GuiComponent) app.getAttribute("component");
			Path instanceFolder = component.getNutchInstance().getInstanceFolder();
			Configuration configuration = component.getNutchInstance().getConfiguration();
			
			Map modifs = (Map)app.getAttribute(PageranksViewer.OPERATIONS_QUEUE_KEY);
			if (modifs == null) {
				modifs = new HashMap<String, Float>();
				app.setAttribute(PageranksViewer.OPERATIONS_QUEUE_KEY, modifs);
			}
			
			synchronized (component) {
				PageranksThread thread = (PageranksThread)app.getAttribute(KEY);
				if(thread == null) {
					thread = new PageranksThread(configuration, instanceFolder, modifs);
					app.setAttribute(KEY, thread);
					thread.setDaemon(true);
					thread.start();
				}
			}
			
			LOG.info("Done.");

	    }
	  }
	
}




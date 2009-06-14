package org.apache.nutch.admin.pageranks;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;


import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.apache.nutch.admin.DefaultGuiComponent;
import org.apache.nutch.admin.GuiComponent;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.searcher.NutchBean.NutchBeanConstructor;
import org.apache.nutch.util.NutchConfiguration;

public class PageranksViewer extends HttpServlet {

	private static final Logger LOG = Logger.getLogger(PageranksViewer.class.getName());
	public static final String OPERATIONS_QUEUE_KEY = "Pagerank_Modifications";
	

	private Configuration configuration;

	public void init() {
		init(NutchConfiguration.create());
	}

	public void init(Configuration conf) {
	}
	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		
		// Initialization 
		GuiComponent component = (GuiComponent) getServletContext().getAttribute("component");
		LOG.info("component : " +  component);
		Path instanceFolder = component.getNutchInstance().getInstanceFolder();
		configuration = component.getNutchInstance().getConfiguration();
		
		Map<String, Float> modifs = (Map<String, Float>) getServletContext().getAttribute(OPERATIONS_QUEUE_KEY);
		if(modifs == null) {
			modifs = new HashMap<String, Float>();
			getServletContext().setAttribute(OPERATIONS_QUEUE_KEY, modifs);
		}
		
		PageranksThread thread = (PageranksThread)getServletContext().getAttribute(PageranksThread.KEY);
		if(thread == null) {
			thread = new PageranksThread(configuration, instanceFolder, modifs);
			getServletContext().setAttribute(PageranksThread.KEY, thread);
		}
		
		out.println("<html><head><title>pagerank fetch...</title></head><body><p>");
		out.flush();
		
		out.println("Showing real-time actions from fetch thread...<br>");
		String msg = "";
		while(true) {
			try { 
				Thread.sleep(100);
				//out.print(".");
				//out.flush();
			} catch (InterruptedException e) { }; 
			if(!msg.equals(thread.getMessage())) {
				msg = thread.getMessage();
				out.println(msg + "<br>");
				out.flush();
			}
			if(!thread.isAlive()) {
				out.println("<br>");
				out.println("Thread terminated.<br>");
				out.println("Final message : " + thread.getMessage() + "<br>");
				break;
			}
		}
		
		
	}
	


	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		doGet(req, resp);
	}



}

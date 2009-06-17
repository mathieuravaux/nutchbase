package org.apache.nutch.admin.scores;

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
//import org.apache.nutch.searcher.NutchBean;
import org.apache.nutchbase.searcher.NutchBeanHbase;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summary;
//import org.apache.nutch.searcher.NutchBean.NutchBeanConstructor;
import org.apache.nutchbase.searcher.NutchBeanHbase.NutchBeanConstructor;
import org.apache.nutchbase.util.hbase.ImmutableRowPart;
import org.apache.nutch.util.NutchConfiguration;

public class UrlWithScore extends HttpServlet {
	private static final Logger LOG = Logger.getLogger(UrlWithScore.class.getName());

	private Configuration configuration;
	public static final int PAGE_SIZE = 20;
	
	public void init() { init(NutchConfiguration.create()); }
	public void init(Configuration conf) { }
	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		//dumpServletInfo(req);
		
		// Initialization 
		GuiComponent component = (GuiComponent) getServletContext().getAttribute("component");
		Path instanceFolder = component.getNutchInstance().getInstanceFolder();
		configuration = component.getNutchInstance().getConfiguration();
		Path crawlDir = new Path(configuration.get("crawl.dir"));
		
		
		// Get a search bean to display results
		NutchBeanHbase bean = NutchBeanHbase.get(getServletContext(), configuration);
		if(getServletContext().getAttribute("forceReload") != null) {
			LOG.info("Forcing crawlDb reload");
			LOG.info("Forcing searcher index reload");
			bean.close();
			bean = null;
			getServletContext().removeAttribute("forceReload");
		}
		if(bean == null) {
			bean = new NutchBeanHbase(configuration, "webtable");
			getServletContext().setAttribute(NutchBeanHbase.KEY, bean);
		}
		System.out.println("Bean : " + bean);
		
		
		int start = 0;
		int hitsPerPage = 30;
		int hitsToRetrieve = hitsPerPage;
		int hitsPerSite = 2;
   	    String sort = null;
   	    boolean reverse = false;
		
		// Treating request parameters
		String query = req.getParameter("query");
		LOG.info("query: " + query);
		if (query == null) { query = ""; }
        Query query2 = Query.parse(query, "", configuration);

		
		ArrayList results = new ArrayList();
		Hits hits;
		try{
		     hits = bean.search(query2, start + hitsToRetrieve, hitsPerSite, "site", sort, reverse);
		} catch (IOException ex){
			 hits = new Hits(0,new Hit[0]);	
		}
		
		int end = (int)Math.min(hits.getLength(), start + hitsPerPage);
		int length = end-start;
		int realEnd = (int)Math.min(hits.getLength(), start + hitsToRetrieve);

		Hit[] show = hits.getHits(start, realEnd-start);
		HitDetails[] details = bean.getDetails(show);
		Summary[] summaries;
		try{
			summaries = bean.getSummary(details, query2);
		}catch(Exception e) {
			LOG.error("Impossible to retrieve summaries : \n " + org.apache.hadoop.util.StringUtils.stringifyException(e));
			summaries = new Summary[show.length];
			for(int i=0; i<show.length; i++) {
				summaries[i] = new Summary();
			}
			
		}
		LOG.info("total hits: " + hits.getTotal());
		
//		HitDetails detail = details[i];
		//String title = detail.getValue("title");
		//String url = detail.getValue("url");
		//String summary = summaries[i].toHtml(true);
		
		ArrayList explanations = new ArrayList<String>();
		ArrayList pageranks = new ArrayList<Float>();
		ArrayList votes = new ArrayList<Float>();
		for (int i = 0; i < show.length; i++) {
			Hit hit = show[i];
			HitDetails detail = details[i];
			String url = detail.getValue("url");
			LOG.debug("#" + i);
			LOG.debug(detail.getValue("title") + "(" + url + ")");
			
			explanations.add( bean.getExplanation(query2, hit) );
			
			LOG.debug("BOOST : " + detail.getValue("boost"));
			Text key = new Text(url);
		    
		    ImmutableRowPart row = bean.getRow(detail);
		    float PR = row.getPagerank();
		    pageranks.add(PR);
		    votes.add(row.getVotes());
			
		}
		
		// display results
		req.setAttribute("results", Arrays.asList(details));
		req.setAttribute("summaries", Arrays.asList(summaries));
		req.setAttribute("explanations", explanations);
		req.setAttribute("pageranks", pageranks);
		req.setAttribute("votes", votes);
		RequestDispatcher view = req.getRequestDispatcher("url_list.jsp");
		view.forward(req, resp);
	}
	


	private void dumpServletInfo(HttpServletRequest req) {
		LOG.info("Request attributes : ");
		Enumeration e = req.getAttributeNames();
		while (e.hasMoreElements()) {
			LOG.info("name : " + (String)e.nextElement());
		}
		LOG.info("---");

		LOG.info("Servlet context attributes : ");
		e = getServletContext().getAttributeNames();
		while (e.hasMoreElements()) {
			LOG.info("name : " + (String)e.nextElement());
		}
		LOG.info("---");
	}
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		doGet(req, resp);
	}



}

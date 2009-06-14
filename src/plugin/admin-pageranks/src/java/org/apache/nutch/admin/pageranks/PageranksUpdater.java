package org.apache.nutch.admin.pageranks;

import java.io.*;
import java.util.*;
import java.net.URL;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.*;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.admin.DefaultGuiComponent;
import org.apache.nutch.admin.GuiComponent;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;


public class PageranksUpdater extends HttpServlet {
	private static final Logger LOG = Logger.getLogger(PageranksUpdater.class.getName());

	static final String URLS_TO_MODIFY = "urlsToModify";
	static final String PAGERANK_PARAM = "pagerank";
	static final String VOTES_PARAM = "votes";

	private Configuration configuration;
	private Path crawlDb;
	private HashMap<String, Float> UrlsToModify = new HashMap<String, Float>();
	boolean crawlDbOpened = false;
	
	public void init() {
		init(NutchConfiguration.create());
	}

	public void init(Configuration conf) {
		configuration = conf; 
	}
	
	
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		
		String url = req.getParameter("url");
		LOG.info("URL for metas modification : " + url);
		
		float newPagerank = Float.valueOf( req.getParameter(PAGERANK_PARAM));
		UrlsToModify.put(url, new Float(newPagerank));
		
		getServletContext().setAttribute(URLS_TO_MODIFY, UrlsToModify);
		
		out.println("{ \"success\": true, \"urlsToModify\" : " + UrlsToModify.size() + " }");
		
	}

	
	
	
	
	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		resp.setContentType("text/html");
		PrintWriter out = resp.getWriter();
		if(req.getParameter("confirmModifications") == null) {
			out.println("confirmModifications param was not given. Skipping modifications...");
			return;
		}
		
		GuiComponent component = (GuiComponent) getServletContext().getAttribute("component");
		Path instanceFolder = component.getNutchInstance().getInstanceFolder();
		
		out.println("<html><head>Score update job</head><body><h3>Scores Job Running</h3><p>");
		out.flush();
		
		if(UrlsToModify.size() == 0) {
			out.println("Nothing to do, quitting.");
			return;
		}
		
		out.println("Creating url update thread...<br>");
		PageranksUpdateThread thread = new PageranksUpdateThread(configuration, instanceFolder, UrlsToModify);
		out.println("Starting url updates...<br>");
		String msg = "";
		getServletContext().setAttribute("forceReload", true);
		thread.start();
		while(true) {
			try { Thread.sleep(1000); out.print("."); out.flush(); } catch (InterruptedException e) { }; 
			if(!msg.equals(thread.getMessage())) {
				out.println("<br>");
				msg = thread.getMessage();
				out.println(msg + "<br>");
				
			}
			if(!thread.isAlive()) {
				out.println("<br>");
				out.println("Job terminated.<br>");
				out.println("Final message : " + thread.getMessage() + "<br>");
				break;
			}
		}
		getServletContext().setAttribute("forceReload", true);
	}
	


}

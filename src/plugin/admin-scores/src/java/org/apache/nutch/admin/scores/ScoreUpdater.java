package org.apache.nutch.admin.scores;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.admin.DefaultGuiComponent;
import org.apache.nutch.admin.GuiComponent;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.nutchbase.util.hbase.TableUtil;


public class ScoreUpdater extends HttpServlet {
	private static final Logger LOG = Logger.getLogger(ScoreUpdater.class.getName());

	static final String URLS_TO_MODIFY = "urlsToModify";
	static final String PAGERANK_PARAM = "pagerank";
	static final String VOTES_PARAM = "votes";

	private Configuration configuration;
	private Path crawlDb;
	private HashMap<String, List<Modification>> UrlsToModify = new HashMap<String, List<Modification>>();
	boolean crawlDbOpened = false;
	
	public void init() {
		init(NutchConfiguration.create());
	}

	public void init(Configuration conf) {
		configuration = conf; 
	}
	
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		Map<String, String[] > params = req.getParameterMap();
		
		String url = (String)req.getParameter("url");
		String reversedUrl = TableUtil.reverseUrl(url);
		System.out.println("URL for metas modification : " + url);
		
		String PR_ = (String)req.getParameter(PAGERANK_PARAM);
		String votes_ = (String)req.getParameter(VOTES_PARAM);
		HTable table = new HTable(new HBaseConfiguration(), "webtable");
		
		RowPart row = new RowPart( table.getRow(reversedUrl) );
		if (PR_ != null) {
			float PR = Float.valueOf(PR_);
			System.out.println("PR :" + PR);
			row.setPagerank(PR);		
		}
		if (votes_ != null) {
			float votes = Float.valueOf(votes_);
			System.out.println("votes :" + votes);
			row.setVotes(votes);		
		}
		table.commit(row.makeBatchUpdate());
		
		out.println("{ \"success\": true }");
		
	}

	
	
	
	
	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		resp.setContentType("text/html");
		PrintWriter out = resp.getWriter();
	}
	


}

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.admin.crawldb;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.*;


import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.log4j.Logger;
import org.apache.nutch.admin.DefaultGuiComponent;
import org.apache.nutch.admin.GuiComponent;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutchbase.util.hbase.TableUtil;

public class UrlWithStatus extends HttpServlet {
	private static final Logger LOG = Logger.getLogger(UrlWithStatus.class.getName());
	public static final int PAGE_SIZE = 100;
	
	private Path crawlDb;
	private Configuration configuration;

	public void init() { init(NutchConfiguration.create()); }
	public void init(Configuration conf) {}
	
	private String testUrl(RowResult rowResult, boolean filterUrl, String urlFilter) {
		if (rowResult == null) {
			return null;
		}
		if (filterUrl) {
			String url = TableUtil.unreverseUrl(Bytes.toString(rowResult.getRow()));
			return url.contains(urlFilter) ? url : null;
		} else {
			return TableUtil.unreverseUrl(Bytes.toString(rowResult.getRow()));
		}
	}

	
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		
		// Initialization 
		GuiComponent component = (GuiComponent) getServletContext().getAttribute("component");
		Path instanceFolder = component.getNutchInstance().getInstanceFolder();
		configuration = component.getNutchInstance().getConfiguration();
		
		// Status filter drop-down
		String statusdrop = req.getParameter("statusdrop");
		LOG.info("Statusdrop : " + statusdrop);
		boolean filterStatus = false;
		byte statusFilter = Byte.MAX_VALUE;
		if (statusdrop == null) {
			statusdrop = "all";
		}
		if (!"".equals(statusdrop) && !"all".equals(statusdrop)) {
			filterStatus = true;
			statusFilter = Byte.parseByte(statusdrop);
		}
		req.setAttribute("statusdrop", statusdrop);
		req.setAttribute("statusFilter", Byte.toString(statusFilter));
		String statusMenu = req.getParameter("status");
		
		String urlFilter = req.getParameter("urlFilter");
		if (urlFilter == null) { urlFilter = ""; }
		urlFilter = urlFilter.toLowerCase();
		boolean filterUrl = !"".equals(urlFilter);
		req.setAttribute("urlFilter", urlFilter);
		System.out.println("urlFilter : (" + Boolean.toString(filterUrl) + ")'" + urlFilter+ "'");
		System.out.println("statusMenu  : " + statusMenu);

		int pageIndex;
		String pageIndexName = req.getParameter("pageIndex");
		if ( pageIndexName == null || "".equals( pageIndexName ) ){
			pageIndex = 0;
		}else{
			pageIndex = Integer.parseInt( pageIndexName );
		}
		req.setAttribute("pageIndex", new Integer(pageIndex));
		System.out.println("pageIndex : " + pageIndex);
		
		
		HTable table = new HTable(new HBaseConfiguration(), "webtable");
		String[] scannedColumns = new String[] {"status:", "score:"  };
		
		
		
		
		// Delete a domain if asked by the user
		String urltodelete = req.getParameter("urltodelete");
		// LOG.info("URL for domain deletion : " + urltodelete);
		if (urltodelete != null) {
			URL u = new URL(urltodelete);
			String domaintodelete= u.getProtocol() + "://" + u.getAuthority();
			// LOG.info("domain for domain deletion : " + domaintodelete);
			String prefix = TableUtil.reverseUrl(domaintodelete);
			Scanner scanner = table.getScanner(scannedColumns);
			for (RowResult rowResult : scanner) {
				String url = Bytes.toString(rowResult.getRow());
				if (url.startsWith(prefix)) {
					table.deleteAll(url);
				}
			}
		}
		
		
		// Get the matching urls
		Scanner scanner = table.getScanner(scannedColumns);
		Map<String, CrawlDatum> map = new TreeMap<String, CrawlDatum>();
		int to_skip = pageIndex * PAGE_SIZE, found_urls = 0;
		System.out.println("Going to skip : " + to_skip + " urls");
		RowResult rowResult;
		String url;
		do {
			rowResult = scanner.next();
			url = testUrl(rowResult, filterUrl, urlFilter);
			if (url != null) {
				to_skip--;
			}
		} while (to_skip > 0 && rowResult != null);
		
//		System.out.println("Urls skipped");
		
		do {
			rowResult = scanner.next();
			url = testUrl(rowResult, filterUrl, urlFilter);
			if (url != null) {
//				System.out.println("url : " + url);
				Byte st = rowResult.get( Bytes.toBytes("status:")).getValue()[0];
				float score = Bytes.toFloat(rowResult.get( Bytes.toBytes("score:")).getValue());
				CrawlDatum crawlDatum = new CrawlDatum((int)st, 0, score);
				map.put(url, crawlDatum);
				found_urls++;
			}
		} while (found_urls < PAGE_SIZE && rowResult != null);
		scanner.close();
		
		
		// display results
		req.setAttribute("map", map);
		RequestDispatcher view = req.getRequestDispatcher("url_list.jsp");
		view.forward(req, resp);
	}
	
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		doGet(req, resp);
	}



}

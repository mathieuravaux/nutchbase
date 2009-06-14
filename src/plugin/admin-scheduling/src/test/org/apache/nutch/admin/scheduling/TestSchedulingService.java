package org.apache.nutch.admin.scheduling;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.util.NutchConfiguration;
import org.quartz.Scheduler;

import junit.framework.TestCase;

public class TestSchedulingService extends TestCase {

	
	private PathSerializable schedulingFolder;
	private SchedulingService schedulerService;
	private Configuration conf;
	private String pathString = "/tmp/TestSchedulingService/";
	
	protected void setUp() throws Exception {
		super.setUp();
		schedulingFolder =  new PathSerializable(new Path(pathString));
		schedulerService = new SchedulingService(schedulingFolder);
		conf = NutchConfiguration.create();
	}
	
	public void testScheduleCronJob() {
		System.out.println("Scheduling Folder : " + schedulingFolder); 
		
		Map<String, PathSerializable> data = new HashMap<String, PathSerializable>();
		data.put("crawldb", new PathSerializable(schedulingFolder, "crawldb"));
		data.put("linkdb", new PathSerializable(schedulingFolder, "linkdb"));
		data.put("segments", new PathSerializable(schedulingFolder, "segments"));
		data.put("configuration", schedulingFolder);
		//cron : sec min hour dayOfMonth Month dayOfWeek Year(optional)
		String time = "0 0 16 22";  
	    String day = "NOV ? 2080";
	   	String cronPattern = time + " " + day;
	   	try {
	   		schedulerService.scheduleCronJob(SchedulingService.CRAWL_JOB, Scheduler.DEFAULT_GROUP, AdminCrawl.class, data, cronPattern);
	   	}
	   	catch (Exception ex) {
			ex.printStackTrace();
			fail(ex.toString());
		}
		
	}
	
	public void tearDown() throws Exception{
		super.tearDown();
		
		FileSystem fs = FileSystem.getNamed("local", conf);
		if(fs.exists(schedulingFolder)) {
			fs.delete(schedulingFolder);
		}
	}
	
}

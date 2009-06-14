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

package org.apache.nutch.admin.scheduling;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nutch.admin.GuiConfigUtil;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.Indexer;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;

public class AdminCrawl implements StatefulJob {

  private static Log LOG = LogFactory.getLog(AdminCrawl.class);	
	
  private static class RunningPathFilter implements PathFilter {
    public boolean accept(Path path) {
      String name = path.getName().toLowerCase();
      return name.endsWith("running");
    }
  }

  public void execute(JobExecutionContext context) throws JobExecutionException {
    JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    Path crawldbFile = (PathSerializable) jobDataMap.get("crawldb");
    Path linkdbFile = (PathSerializable) jobDataMap.get("linkdb");
    Path segments = (PathSerializable) jobDataMap.get("segments");
    Path segment = null;

    Path configurationFile = (PathSerializable) jobDataMap.get("configuration");
    
    Configuration configuration = null;
    
    try {

     configuration = 
       GuiConfigUtil.loadNewConfiguration(configurationFile);
    	
      segment = generateSegment(crawldbFile, segments, configuration);
      fetchSegment(segment, configuration);
      updateCrawldb(crawldbFile, segment, configuration);
      updateLinkDb(linkdbFile, new Path[] { segment }, configuration);
      index(
          new Path(segment, "index"), 
          crawldbFile, 
          linkdbFile,
          new Path[] { segment }, 
          configuration
          );

    } catch (Exception e) {
      throw new JobExecutionException(e.getMessage());
    } finally {
      try {
        FileSystem fileSystem = FileSystem.get(configuration);
        RunningPathFilter filter = new RunningPathFilter();
        // deleteFiles(fileSystem.listPaths(crawldbFile, filter), fileSystem);
        // deleteFiles(fileSystem.listPaths(linkdbFile, filter), fileSystem);
        // deleteFiles(fileSystem.listPaths(segments, filter), fileSystem);
        // deleteFiles(fileSystem.listPaths(segment, filter), fileSystem);
        // deleteFiles(fileSystem.listPaths(new Path(segment, "index"), filter), fileSystem);
        deleteFiles(fileSystem.listStatus(crawldbFile, filter), fileSystem);
        deleteFiles(fileSystem.listStatus(linkdbFile, filter), fileSystem);
        deleteFiles(fileSystem.listStatus(segments, filter), fileSystem);
        deleteFiles(fileSystem.listStatus(segment, filter), fileSystem);
        deleteFiles(fileSystem.listStatus(new Path(segment, "index"), filter), fileSystem);
      } catch (IOException e) {
        throw new JobExecutionException(e.getMessage());
      }
    }

  }


  private void deleteFiles(Path[] files, FileSystem fileSystem)
      throws IOException {
    for (int i = 0; i < files.length; i++) {
      Path file = files[i];
      fileSystem.delete(file);
    }

  }
  
  private void deleteFiles(FileStatus[] statuses, FileSystem fileSystem)
      throws IOException {
    for (int i = 0; i < statuses.length; i++) {
      Path file = statuses[i].getPath();
      fileSystem.delete(file);
    }

  }

  
  private void index(Path index, Path crawldbFile, Path linkdbFile,
      Path[] segments, Configuration configuration) throws IOException {
    FileSystem fileSystem = FileSystem.get(configuration);
    // create running files in segments
    for (int i = 0; i < segments.length; i++) {
      Path file = segments[i];
      fileSystem.createNewFile(new Path(file, "index.running"));
    }
    // create running files in linkdb
    fileSystem.createNewFile(new Path(linkdbFile, "index.running"));
    // create running files in crawldb
    fileSystem.createNewFile(new Path(crawldbFile, "index.running"));
    Indexer indexer = new Indexer(configuration);
    indexer.index(index, crawldbFile, linkdbFile, Arrays.asList(segments));
  }


  private void updateLinkDb(Path linkdbFile, Path[] segments,
      Configuration configuration) throws IOException {

    FileSystem fileSystem = FileSystem.get(configuration);

    Path running = new Path(linkdbFile, "linkdb.running");
    // create lock files in segments
    for (int i = 0; i < segments.length; i++) {
      Path file = segments[i];
      fileSystem.createNewFile(new Path(file, "linkdb.running"));
    }
    // create lockfile in linkdb
    fileSystem.createNewFile(running);

    LinkDb linkDb = new LinkDb(configuration);
    linkDb.invert(linkdbFile, segments,true, true, false);

    for (int i = 0; i < segments.length; i++) {
      Path file = segments[i];
      fileSystem.createNewFile(new Path(file, "invert.done"));
    }
  }


  private void updateCrawldb(Path crawldbFile, Path segment,
      Configuration configuration) throws IOException {
    FileSystem fileSystem = FileSystem.get(configuration);
    Path runningSegment = new Path(segment, "crawldb.running");
    Path runningDB = new Path(crawldbFile, "crawldb.running");
    fileSystem.createNewFile(runningSegment);
    fileSystem.createNewFile(runningDB);
    CrawlDb crawlDb = new CrawlDb(configuration);
    // crawlDb.update(crawldbFile, segment, true, true);
    Path[] segments = new Path[1];
    segments[0] = segment;
    crawlDb.update(crawldbFile, segments, true, true);
  }

 
  private void fetchSegment(Path segment, Configuration configuration)
      throws IOException {
    FileSystem fileSystem = FileSystem.get(configuration);
    Path running = new Path(segment, "fetch.running");
    fileSystem.createNewFile(running);
    Fetcher fetcher = new Fetcher(configuration);
    fetcher.fetch(segment, configuration.getInt("fetcher.threads.fetch", 10), true);
    fileSystem.createNewFile(new Path(segment, "fetch.done"));
    if (configuration.getBoolean("fetcher.parse", true)) {
      fileSystem.createNewFile(new Path(segment, "parse.done"));
    }

  }

  private Path generateSegment(Path crawldbFile, Path segments,
      Configuration configuration) throws IOException {
    FileSystem system = FileSystem.get(configuration);
    Path runningGenerateSegment = new Path(segments, "generate.running");
    Path runningGenerateDB = new Path(crawldbFile, "generate.running");
    system.createNewFile(runningGenerateSegment);
    system.createNewFile(runningGenerateDB);
    Generator generator = new Generator(configuration);
    // Path segment = generator.generate(crawldbFile, segments);
    long topN = Long.MAX_VALUE;
    Path segment = generator.generate(crawldbFile, segments, -1, topN, System.currentTimeMillis());
    return segment;
  }

}

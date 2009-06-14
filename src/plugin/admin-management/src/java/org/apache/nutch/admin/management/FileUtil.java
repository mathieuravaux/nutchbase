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

package org.apache.nutch.admin.management;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseOutputFormat;


public class FileUtil {

  private static Log LOG = LogFactory.getLog(FileUtil.class);

  private static class RunningPathFilter implements PathFilter {
   
    public boolean accept(Path file) {
      String name = file.getName().toLowerCase();
      return name.endsWith("running");
    }
    
  }

  private static class DirectoryPathFilter implements PathFilter {

    private FileSystem fFileSystem;
   
    public DirectoryPathFilter(FileSystem fileSystem) {
      this.fFileSystem = fileSystem;
    }

    public boolean accept(Path file) {
      boolean ret = false;
      try {
        ret = this.fFileSystem.isDirectory(file);
      } catch (IOException e) {
        LOG.warn(e.toString());
      }
      return ret;
    }
  }

  
  public static long size(Path folder, Configuration configuration)
      throws IOException {
    
    FileSystem fileSystem = FileSystem.get(configuration);
    // Path[] files = fileSystem.listPaths(folder);
    FileStatus[] filestatuses = fileSystem.listStatus(folder);
    int len = filestatuses.length;
    Path[] files = new Path[len];
	for (int i=0; i < len; i++) {
	    files[i] = filestatuses[i].getPath();
	}
    
    long size = 0;
    for (int i = 0; files != null && i < files.length; i++) {
      Path file = files[i];
      if (fileSystem.isDirectory(file)) {
        size = size + size(file, configuration);
      }
      size = size + fileSystem.getLength(file);
    }
    return size + fileSystem.getLength(folder);
  }

  /**
   * @return true if fetch.done exists
   */
  public static boolean isFetched(Path segment, Configuration configuration)
      throws IOException {
    
	  //return exists(configuration, segment, "fetch.done");
	  FileSystem fs = FileSystem.get(configuration);
	  return fs.exists(new Path(segment, CrawlDatum.FETCH_DIR_NAME));
  }

  /**
   * @return true if invert.done exists
   */
  public static boolean isInverted(Path segment, Configuration configuration)
      throws IOException {

	  return exists(configuration, segment, "invert.done");
	  //FileSystem fs = FileSystem.get(configuration);
	  //return fs.exists(new Path(segment, CrawlDatum.));
  }

  /**
   * @return true if parse.done exists
   */
  public static boolean isParsed(Path segment, Configuration configuration)
      throws IOException {
    
      //return exists(configuration, segment, "parse.done");
	  FileSystem fs = FileSystem.get(configuration);
	  return fs.exists(new Path(segment, CrawlDatum.PARSE_DIR_NAME));
  }

  /**
   * @return true if parse.done exists
   */
  public static boolean isIndexed(Path segment, Configuration configuration)
      throws IOException {
    
    FileSystem system = FileSystem.get(configuration);
    // Path[] files = system.listPaths(new Path(segment, "index"));
    FileStatus[] filestatuses = system.listStatus(new Path(segment, "index"));
    int len = filestatuses.length;
    Path[] files = new Path[len];
	for (int i=0; i < len; i++) {
	    files[i] = filestatuses[i].getPath();
	}
    
    
    boolean ret = false;
    for (int i = 0; i < files.length; i++) {
      //e.g. file = part-00000
      Path file = files[i];
      if(system.isDirectory(file) && file.getName().startsWith("part-")) {
        ret = exists(configuration, file, "index.done");
        if(!ret) {
          break;
        }
      }
    }
    return ret;
  }

  /**
   * @return true if parse.done exists
   */
  public static boolean isInjected(Path instanceFolder,
      Configuration configuration) throws IOException {
	  
	  Path crawlDir = new Path(configuration.get("crawl.dir"));
	  return exists(configuration, crawlDir, "crawldb");
  }

  /**
   * @return true if search.done exists
   */
  public static boolean isReadyToSearch(Path segment,
      Configuration configuration) throws IOException {
    
    return exists(configuration, segment, "search.done");
  }

  /**
   * @return true if fileName in folder exists
   */
  private static boolean exists(Configuration configuration, Path folder,
      String fileName) throws IOException {
    FileSystem fileSystem = FileSystem.get(configuration);
    return fileSystem.exists(new Path(folder, fileName));
  }

  /**
   * @return true if parse.done exists
   */
  public static List<String> getRunningFiles(Path folder, Configuration configuration)
      throws IOException {
    FileSystem fileSystem = FileSystem.get(configuration);
    // Path[] files = fileSystem.listPaths(folder, new RunningPathFilter());
    FileStatus[] filestatuses = fileSystem.listStatus(folder, new RunningPathFilter());
    int len = filestatuses.length;
    Path[] files = new Path[len];
	for (int i=0; i < len; i++) {
	    files[i] = filestatuses[i].getPath();
	}
    
    
    List<String> list = new LinkedList<String>();
    for (int i = 0; i < files.length; i++) {
      Path file = files[i];
      list.add(file.getName());
    }
    return list;
  }
  
  /**
   * @return  folders in this folder
   */
  public static Path[] listFolders(Path folder, 
      Configuration configuration) 
      throws IOException {
    
    FileSystem system = FileSystem.get(configuration);
    // return system.listPaths(folder, new DirectoryPathFilter(system));
    FileStatus[] filestatuses = system.listStatus(folder, new DirectoryPathFilter(system));
    int len = filestatuses.length;
    Path[] files = new Path[len];
	for (int i=0; i < len; i++) {
	    files[i] = filestatuses[i].getPath();
	}
    return files;
  }

}

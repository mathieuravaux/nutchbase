/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http:www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.admin.system;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.dfs.DFSClient;
// import org.apache.hadoop.dfs.*;
// import org.apache.hadoop.dfs.DistributedFileSystem;
// import org.apache.hadoop.fs.DistributedFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;




public class SystemUtil {
    
	
	private DatanodeInfo[] fDataNodesInfos;
 
    private long capacity;
 
    private long available;
 
    public SystemUtil(Configuration configuration, Path instanceFolder)
       throws IOException {
 
     FileSystem fileSystem = FileSystem.get(configuration);
     if (!"local".equals(fileSystem.getName())) {
       DistributedFileSystem dfsfs = (DistributedFileSystem) fileSystem;
       DFSClient dfs = dfsfs.getClient();
       //this.fDataNodesInfos = dfs.datanodeReport();
     } else {
       df(instanceFolder.toString());
       
       //TODO find a way to get storageID and Xceiver
       //this.fDataNodesInfos =
       //    new DatanodeInfo[] { new DatanodeDescriptor(new DatanodeID(
       //        "local","storageId", 10000), this.capacity, this.available, 1)};
     }
   }
 
   private void df(String dir) throws IOException {
     Process process =
         Runtime.getRuntime().exec(new String[] { "df", "-k", dir });
 
     try {
       if (process.waitFor() == 0) {
         BufferedReader lines =
             new BufferedReader(new InputStreamReader(process.getInputStream()));
 
         lines.readLine();  // skip headings
 
         StringTokenizer tokens =
             new StringTokenizer(lines.readLine(), " \t\n\r\f%");
 
         tokens.nextToken(); // skip filesystem
         if (!tokens.hasMoreTokens())  // for long filesystem name
           tokens = new StringTokenizer(lines.readLine(), " \t\n\r\f%");
         this.capacity = Long.parseLong(tokens.nextToken()) * 1024;
         tokens.nextToken(); // skip used
         this.available = Long.parseLong(tokens.nextToken()) * 1024;
         tokens.nextToken(); // skip percent used
         tokens.nextToken(); // skip mount
 
       } else {
         throw new IOException(new BufferedReader(new InputStreamReader(process
             .getErrorStream())).readLine());
       }
     } catch (InterruptedException e) {
       throw new IOException(e.toString());
     } finally {
       process.destroy();
     }
   }
 
   public DatanodeInfo[] getDataNodesInfos() {
     return this.fDataNodesInfos;
   }
 
   /**
    * Return an abbreviated English-language description of the byte length
    * 
    * @param len
    * @return the human readable of the size
    */
   public static String byteDesc(long len) {
     double val = 0.0;
     String ending = "";
     if (len < 1024 * 1024) {
       val = (1.0 * len) / 1024;
       ending = " k";
     } else if (len < 1024 * 1024 * 1024) {
       val = (1.0 * len) / (1024 * 1024);
       ending = " Mb";
     } else {
       val = (1.0 * len) / (1024 * 1024 * 1024);
       ending = " Gb";
     }
     return limitDecimal(val, 2) + ending;
   }
 
   private static String limitDecimal(double d, int placesAfterDecimal) {
     String strVal = Double.toString(d);
     int decpt = strVal.indexOf(".");
     if (decpt >= 0) {
       strVal =
           strVal.substring(
               0, 
               Math.min(strVal.length(), decpt + 1 + placesAfterDecimal)
               );
     }
     return strVal;
   }
 
 }

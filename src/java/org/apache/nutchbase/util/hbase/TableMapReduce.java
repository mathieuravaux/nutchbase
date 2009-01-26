/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutchbase.util.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapred.TableReduce;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("unchecked")
public abstract class TableMapReduce<K extends WritableComparable, V extends Writable>
extends Configured
implements TableMap<K, V>, TableReduce<K, V>, Closeable, JobConfigurable {
  
  /**
   * Use this before submitting a TableMapReduce job. It will
   * appropriately set up the JobConf.
   * 
   * @param table
   * @param reducer
   * @param job
   */
  public static void initJob(String table, String columns,
      Class<? extends TableMapReduce> mrClass, 
      Class<? extends WritableComparable> outputKeyClass, 
      Class<? extends Writable> outputValueClass,
      JobConf job) throws IOException {
    TableMapReduceUtil.initTableMapJob(table, columns, mrClass,
                                       outputKeyClass, outputValueClass, job);
    TableMapReduceUtil.initTableReduceJob(table, mrClass, job);
  }

  /**
   * Call a user defined function on a single HBase record, represented
   * by a key and its associated record value.
   * 
   * @param key
   * @param value
   * @param output
   * @param reporter
   * @throws IOException
   */
  public abstract void map(ImmutableBytesWritable key, RowResult value,
      OutputCollector<K, V> output, Reporter reporter) throws IOException;

  /**
   * 
   * @param key
   * @param values
   * @param output
   * @param reporter
   * @throws IOException
   */
  public abstract void reduce(K key, Iterator<V> values,
    OutputCollector<ImmutableBytesWritable, BatchUpdate> output, Reporter reporter)
  throws IOException;

  /** Default implementation that does nothing. */
  public void close() throws IOException {
  }

  /** Default implementation that does nothing. */
  public void configure(JobConf job) {
  }
}

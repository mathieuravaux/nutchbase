/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutchbase.indexer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.lucene.document.Document;

import org.apache.nutch.plugin.*;
import org.apache.nutch.util.ObjectCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutchbase.util.hbase.ImmutableRowPart;

/** Creates and caches {@link IndexingFilter} implementing plugins.*/
public class IndexingFiltersHbase {

  public static final String INDEXINGFILTER_ORDER = "indexingfilterhbase.order";

  public final static Log LOG = LogFactory.getLog(IndexingFiltersHbase.class);

  private IndexingFilterHbase[] indexingFilters;

  public IndexingFiltersHbase(Configuration conf) {
    /* Get indexingfilter.order property */
    String order = conf.get(INDEXINGFILTER_ORDER);
    ObjectCache objectCache = ObjectCache.get(conf);
    this.indexingFilters = (IndexingFilterHbase[]) objectCache
        .getObject(IndexingFilterHbase.class.getName());
    if (this.indexingFilters == null) {
      /*
       * If ordered filters are required, prepare array of filters based on
       * property
       */
      String[] orderedFilters = null;
      if (order != null && !order.trim().equals("")) {
        orderedFilters = order.split("\\s+");
      }
      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(
            IndexingFilterHbase.X_POINT_ID);
        if (point == null)
          throw new RuntimeException(IndexingFilterHbase.X_POINT_ID + " not found.");
        Extension[] extensions = point.getExtensions();
        HashMap<String, IndexingFilterHbase> filterMap =
          new HashMap<String, IndexingFilterHbase>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          IndexingFilterHbase filter = (IndexingFilterHbase) extension
              .getExtensionInstance();
          LOG.info("Adding " + filter.getClass().getName());
          if (!filterMap.containsKey(filter.getClass().getName())) {
            filterMap.put(filter.getClass().getName(), filter);
          }
        }
        /*
         * If no ordered filters required, just get the filters in an
         * indeterminate order
         */
        if (orderedFilters == null) {
          objectCache.setObject(IndexingFilterHbase.class.getName(),
              filterMap.values().toArray(
                  new IndexingFilterHbase[0]));
          /* Otherwise run the filters in the required order */
        } else {
          ArrayList<IndexingFilterHbase> filters = new ArrayList<IndexingFilterHbase>();
          for (int i = 0; i < orderedFilters.length; i++) {
            IndexingFilterHbase filter = filterMap.get(orderedFilters[i]);
            if (filter != null) {
              filters.add(filter);
            }
          }
          objectCache.setObject(IndexingFilterHbase.class.getName(), filters
              .toArray(new IndexingFilterHbase[filters.size()]));
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      this.indexingFilters = (IndexingFilterHbase[]) objectCache
          .getObject(IndexingFilterHbase.class.getName());
    }
  }                  

  /** Run all defined filters. */
  public Document filter(Document doc, String url, ImmutableRowPart row)
  throws IndexingException {
    for (IndexingFilterHbase indexingFilter : indexingFilters) {
      doc = indexingFilter.filter(doc, url, row);
      // break the loop if an indexing filter discards the doc
      if (doc == null) return null;
    }

    return doc;
  }

  public Set<String> getColumnSet() {
    Set<String> columnSet = new HashSet<String>();
    for (IndexingFilterHbase indexingFilter : indexingFilters) {
      columnSet.addAll(indexingFilter.getColumnSet());
    }
    return columnSet;  
  }
  
}

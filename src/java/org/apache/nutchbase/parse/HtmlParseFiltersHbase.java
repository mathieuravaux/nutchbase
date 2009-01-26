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

package org.apache.nutchbase.parse;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.plugin.*;
import org.apache.nutch.util.ObjectCache;
import org.apache.nutchbase.util.hbase.RowPart;
import org.apache.hadoop.conf.Configuration;

import org.w3c.dom.DocumentFragment;

/** Creates and caches {@link HtmlParseFilter} implementing plugins.*/
public class HtmlParseFiltersHbase {

  private HtmlParseFilterHbase[] htmlParseFilters;

  public HtmlParseFiltersHbase(Configuration conf) {
        ObjectCache objectCache = ObjectCache.get(conf);
        this.htmlParseFilters =
          (HtmlParseFilterHbase[]) objectCache.getObject(HtmlParseFilterHbase.class.getName());
        if (htmlParseFilters == null) {
            HashMap<String, HtmlParseFilterHbase> filters =
              new HashMap<String, HtmlParseFilterHbase>();
            try {
                ExtensionPoint point =
                  PluginRepository.get(conf).getExtensionPoint(HtmlParseFilterHbase.X_POINT_ID);
                if (point == null)
                    throw new RuntimeException(HtmlParseFilterHbase.X_POINT_ID + " not found.");
                Extension[] extensions = point.getExtensions();
                for (int i = 0; i < extensions.length; i++) {
                    Extension extension = extensions[i];
                    HtmlParseFilterHbase parseFilter =
                      (HtmlParseFilterHbase) extension.getExtensionInstance();
                    if (!filters.containsKey(parseFilter.getClass().getName())) {
                        filters.put(parseFilter.getClass().getName(), parseFilter);
                    }
                }
                HtmlParseFilterHbase[] htmlParseFilters =
                  filters.values().toArray(new HtmlParseFilterHbase[filters.size()]);
                objectCache.setObject(HtmlParseFilterHbase.class.getName(), htmlParseFilters);
            } catch (PluginRuntimeException e) {
                throw new RuntimeException(e);
            }
            this.htmlParseFilters =
              (HtmlParseFilterHbase[])objectCache.getObject(HtmlParseFilterHbase.class.getName());
        }
    }                  

  /** Run all defined filters. */
  public ParseHbase filter(String url, RowPart row, ParseHbase parse,
                           HTMLMetaTags metaTags, DocumentFragment doc) {

    // loop on each filter
    for (HtmlParseFilterHbase htmlParseFilter : htmlParseFilters) {
      // call filter interface
      parse = htmlParseFilter.filter(url, row, parse, metaTags, doc);

      // any failure on parse obj, return
      if (!parse.getParseStatus().isSuccess()) {
        return parse;
      }
    }

    return parse;
  }

  public Set<String> getColumnSet() {
    Set<String> columnSet = new HashSet<String>();
    for (HtmlParseFilterHbase htmlParseFilter : htmlParseFilters) {
      columnSet.addAll(htmlParseFilter.getColumnSet());
    }
    return columnSet;
  }
}

package org.apache.nutchbase.parse;

import org.w3c.dom.DocumentFragment;

// Hadoop imports
import org.apache.hadoop.conf.Configurable;

// Nutch imports
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutchbase.plugin.PluggableHbase;
import org.apache.nutchbase.util.hbase.RowPart;


/** Extension point for DOM-based HTML parsers.  Permits one to add additional
 * metadata to HTML parses.  All plugins found which implement this extension
 * point are run sequentially on the parse.
 */
public interface HtmlParseFilterHbase extends PluggableHbase, Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = HtmlParseFilterHbase.class.getName();

  /** Adds metadata or otherwise modifies a parse of HTML content, given
   * the DOM tree of a page. */
  ParseHbase filter(String url, RowPart row, ParseHbase parseResult,
                    HTMLMetaTags metaTags, DocumentFragment doc);
  
}

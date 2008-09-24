package org.apache.nutchbase.util;

import org.apache.nutchbase.util.hbase.TableUtil;

import junit.framework.TestCase;

public class TestTableUtil extends TestCase {
  
  String urlString1 = "http://foo.com/";
  String urlString2 = "http://foo.com:8900/";
  String urlString3 = "ftp://bar.baz.com/";
  String urlString4 = "http://bar.baz.com:8983/to/index.html?a=b&c=d";
  
  String reversedUrlString1 = "com.foo:http/";
  String reversedUrlString2 = "com.foo:http:8900/";
  String reversedUrlString3 = "com.baz.bar:ftp/";
  String reversedUrlString4 = "com.baz.bar:http:8983/to/index.html?a=b&c=d";
  
  public void testReverseUrl() throws Exception {
    assertEquals(TableUtil.reverseUrl(urlString1), reversedUrlString1);
    assertEquals(TableUtil.reverseUrl(urlString2), reversedUrlString2);
    assertEquals(TableUtil.reverseUrl(urlString3), reversedUrlString3);
    assertEquals(TableUtil.reverseUrl(urlString4), reversedUrlString4); 
  }
  
  public void testUnreverseUrl() throws Exception {
    assertEquals(TableUtil.unreverseUrl(reversedUrlString1), urlString1);
    assertEquals(TableUtil.unreverseUrl(reversedUrlString2), urlString2);
    assertEquals(TableUtil.unreverseUrl(reversedUrlString3), urlString3);
    assertEquals(TableUtil.unreverseUrl(reversedUrlString4), urlString4);
  }
}

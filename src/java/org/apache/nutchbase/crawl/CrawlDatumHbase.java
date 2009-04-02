package org.apache.nutchbase.crawl;

import java.util.HashMap;
import java.util.Map;

public class CrawlDatumHbase {
  /** Page was not fetched yet. */
  public static final byte STATUS_UNFETCHED      = 0x01;
  /** Page was successfully fetched. */
  public static final byte STATUS_FETCHED        = 0x02;
  /** Page no longer exists. */
  public static final byte STATUS_GONE           = 0x03;
  /** Page temporarily redirects to other page. */
  public static final byte STATUS_REDIR_TEMP     = 0x04;
  /** Page permanently redirects to other page. */
  public static final byte STATUS_REDIR_PERM     = 0x05;
  /** Fetching unsuccessful, needs to be retried (transient errors). */
  public static final byte STATUS_RETRY          = 0x22;
  /** Fetching successful - page is not modified. */
  public static final byte STATUS_NOTMODIFIED    = 0x26;
  
  private static final Map<Byte, String> NAMES = new HashMap<Byte, String>();
  
  static {
    NAMES.put(STATUS_UNFETCHED, "status_unfetched");
    NAMES.put(STATUS_FETCHED, "status_fetched");
    NAMES.put(STATUS_GONE, "status_gone");
    NAMES.put(STATUS_REDIR_TEMP, "status_redir_temp");
    NAMES.put(STATUS_REDIR_PERM, "status_redir_perm");
    NAMES.put(STATUS_RETRY, "status_retry");
    NAMES.put(STATUS_NOTMODIFIED, "status_notmodified");
  }
  
  public static String getName(byte status) {
    return NAMES.get(status);
  }
 
}

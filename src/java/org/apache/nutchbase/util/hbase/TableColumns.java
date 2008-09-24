package org.apache.nutchbase.util.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class TableColumns {
  public static final String BASE_URL_STR         = "baseUrl:";
  public static final String STATUS_STR           = "status:";
  public static final String FETCH_TIME_STR       = "fetchTime:";
  public static final String RETRIES_STR          = "retries:";
  public static final String FETCH_INTERVAL_STR   = "fetchInterval:";
  public static final String SCORE_STR            = "score:";
  public static final String MODIFIED_TIME_STR    = "modifiedTime:";
  public static final String SIGNATURE_STR        = "signature:";
  public static final String CONTENT_STR          = "content:";
  public static final String CONTENT_TYPE_STR     = "contentType:";
  public static final String TITLE_STR            = "title:";
  public static final String OUTLINKS_STR         = "outlinks:";
  public static final String INLINKS_STR          = "inlinks:";
  public static final String PARSE_STATUS_STR     = "parseStatus:";
  public static final String PROTOCOL_STATUS_STR  = "protocolStatus:";
  public static final String TEXT_STR             = "text:";
  public static final String REPR_URL_STR         = "reprUrl:";
  public static final String METADATA_STR         = "metadata:";
  
  public static final byte[] BASE_URL          = Bytes.toBytes(BASE_URL_STR);
  public static final byte[] STATUS            = Bytes.toBytes(STATUS_STR);
  public static final byte[] FETCH_TIME        = Bytes.toBytes(FETCH_TIME_STR);
  public static final byte[] RETRIES           = Bytes.toBytes(RETRIES_STR);
  public static final byte[] FETCH_INTERVAL    = Bytes.toBytes(FETCH_INTERVAL_STR);
  public static final byte[] SCORE             = Bytes.toBytes(SCORE_STR);
  public static final byte[] MODIFIED_TIME     = Bytes.toBytes(MODIFIED_TIME_STR);
  public static final byte[] SIGNATURE         = Bytes.toBytes(SIGNATURE_STR);
  public static final byte[] CONTENT           = Bytes.toBytes(CONTENT_STR);
  public static final byte[] CONTENT_TYPE      = Bytes.toBytes(CONTENT_TYPE_STR);
  public static final byte[] TITLE             = Bytes.toBytes(TITLE_STR);
  public static final byte[] OUTLINKS          = Bytes.toBytes(OUTLINKS_STR);
  public static final byte[] INLINKS           = Bytes.toBytes(INLINKS_STR);
  public static final byte[] PARSE_STATUS      = Bytes.toBytes(PARSE_STATUS_STR);
  public static final byte[] PROTOCOL_STATUS   = Bytes.toBytes(PROTOCOL_STATUS_STR);
  public static final byte[] TEXT              = Bytes.toBytes(TEXT_STR);
  public static final byte[] REPR_URL          = Bytes.toBytes(REPR_URL_STR);
  public static final byte[] METADATA          = Bytes.toBytes(METADATA_STR);
}

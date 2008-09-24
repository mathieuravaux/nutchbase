package org.apache.nutchbase.util.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.ProtocolStatus;

public class ImmutableRowPart implements Writable {
  
  protected static final int INLINKS_STR_LEN  = 
    TableColumns.INLINKS_STR.length();
  protected static final int OUTLINKS_STR_LEN = 
    TableColumns.OUTLINKS_STR.length();
  
  protected RowResult rowResult;
  
  public ImmutableRowPart() { rowResult = new RowResult(); }
  
  public ImmutableRowPart(RowResult rowResult) {
    this.rowResult = rowResult;
  }
  
  protected String stringify(Cell c) {
    return Bytes.toString(c.getValue());
  }

  public ImmutableRowPart(final byte[] rowId) {
    rowResult = new RowResult(rowId, new HbaseMapWritable<byte[], Cell>());
  }

  public void readFields(DataInput in) throws IOException {
    rowResult.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    rowResult.write(out);
  }
  
  public byte[] getRowId() {
    return rowResult.getRow();
  }
  
  /** Checks if row has the specified column.
   * 
   * @param col Column to be checked
   * @return true if given column exists in row
   */
  public boolean hasColumn(byte[] col) {
    return rowResult.containsKey(col);
  }

  public String getBaseUrl() {
    return stringify(rowResult.get(TableColumns.BASE_URL));
  }

  public byte getStatus() {
    return rowResult.get(TableColumns.STATUS).getValue()[0];
  }
  
  public byte[] getSignature() {
    return rowResult.get(TableColumns.SIGNATURE).getValue();
  }

  public long getFetchTime() {
    return Bytes.toLong(rowResult.get(TableColumns.FETCH_TIME).getValue());
  }
  
  public long getModifiedTime() {
    return Bytes.toLong(rowResult.get(TableColumns.MODIFIED_TIME).getValue());
  }

  public int getFetchInterval() {
    return Bytes.toInt(rowResult.get(TableColumns.FETCH_INTERVAL).getValue());
  }

  public int getRetriesSinceFetch() {
    return Bytes.toInt(rowResult.get(TableColumns.RETRIES).getValue());
  }
  
  public ProtocolStatus getProtocolStatus() {
    ProtocolStatus protocolStatus = new ProtocolStatus();
    byte[] val = rowResult.get(TableColumns.PROTOCOL_STATUS).getValue();
    try {
      return (ProtocolStatus) Writables.getWritable(val, protocolStatus);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public float getScore() {
    return TableUtil.toFloat(rowResult.get(TableColumns.SCORE).getValue());
  }
  
  public byte[] getContent() {
    return rowResult.get(TableColumns.CONTENT).getValue();
  }
  
  public String getContentType() {
    return stringify(rowResult.get(TableColumns.CONTENT_TYPE));
  }

  public String getText() {
    return stringify(rowResult.get(TableColumns.TEXT));
  }
  
  public String getTitle() {
    return stringify(rowResult.get(TableColumns.TITLE));
  }
  
  public ParseStatus getParseStatus() {
    ParseStatus parseStatus = new ParseStatus();
    byte[] val = rowResult.get(TableColumns.PARSE_STATUS).getValue();
    try {
      return (ParseStatus) Writables.getWritable(val, parseStatus);
    } catch (IOException e) {
      return null;
    }
  }
  
  public String getReprUrl() {
    return stringify(rowResult.get(TableColumns.REPR_URL));
  }
  
  public Collection<Outlink> getOutlinks() {
    List<Outlink> outlinks = new ArrayList<Outlink>();
    for (byte[] col : rowResult.keySet()) {
      String column = Bytes.toString(col);
      if (column.startsWith(TableColumns.OUTLINKS_STR)) {
        String toUrl = column.substring(OUTLINKS_STR_LEN);
        String anchor = Bytes.toString(rowResult.get(col).getValue());
        outlinks.add(new Outlink(toUrl, anchor));
      }
    }
    return outlinks;
  }
  
  public Collection<Inlink> getInlinks() {
    List<Inlink> inlinks = new ArrayList<Inlink>();
    for (byte[] col : rowResult.keySet()) {
      String column = Bytes.toString(col);
      if (column.startsWith(TableColumns.INLINKS_STR)) {
        String fromUrl = column.substring(INLINKS_STR_LEN);
        String anchor = Bytes.toString(rowResult.get(col).getValue());
        inlinks.add(new Inlink(fromUrl, anchor));
      }
    }
    
    return inlinks;
  }
  
  /** Checks if a metadata key exists in "metadata" column.
   * @param row Row from hbase
   * @param metaKey Key to search in metadata column
   * @return true if key exists
   */
  public boolean hasMeta(String metaKey) {
    return hasColumn(Bytes.toBytes(TableColumns.METADATA_STR  + metaKey));
  }

  /** Read a metadata key from "metadata" column.
   * @param metaKey Key to search in metadata column
   * @return Value in byte array form
   */
  public byte[] getMeta(String metaKey) {
    return rowResult.get(TableColumns.METADATA_STR + metaKey).getValue();
  }  

}

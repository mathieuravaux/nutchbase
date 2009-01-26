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

public class ImmutableRowPart implements Writable, TableColumns {

  protected static final int INLINKS_STR_LEN  = INLINKS_STR.length();
  protected static final int OUTLINKS_STR_LEN = OUTLINKS_STR.length();

  protected RowResult rowResult;

  /** For Writable. Do not use directly. */
  public ImmutableRowPart() { rowResult = new RowResult(); }

  public ImmutableRowPart(RowResult rowResult) {
    this.rowResult = rowResult;
  }

  protected String stringify(Cell c) {
    if (c == null)
      return null;
    return Bytes.toString(c.getValue());
  }

  protected byte[] get(byte[] column) {
    final Cell c = rowResult.get(column);
    return c == null ? null : c.getValue();
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
    return stringify(rowResult.get(BASE_URL));
  }

  public byte getStatus() {
    return rowResult.get(STATUS).getValue()[0];
  }

  public byte[] getSignature() {
    if (!hasColumn(SIGNATURE))
      return null;
    return rowResult.get(SIGNATURE).getValue();
  }

  public byte[] getPrevSignature() {
    if (!hasColumn(PREV_SIGNATURE))
      return null;
    return rowResult.get(PREV_SIGNATURE).getValue();
  }

  public long getFetchTime() {
    return Bytes.toLong(rowResult.get(FETCH_TIME).getValue());
  }

  public long getPrevFetchTime() {
    Cell c = rowResult.get(PREV_FETCH_TIME);
    if (c == null)
      return 0L;

    return Bytes.toLong(c.getValue());
  }

  public long getModifiedTime() {
    return Bytes.toLong(rowResult.get(MODIFIED_TIME).getValue());
  }

  public int getFetchInterval() {
    return Bytes.toInt(rowResult.get(FETCH_INTERVAL).getValue());
  }

  public int getRetriesSinceFetch() {
    return Bytes.toInt(rowResult.get(RETRIES).getValue());
  }

  public ProtocolStatus getProtocolStatus() {
    final ProtocolStatus protocolStatus = new ProtocolStatus();
    final byte[] val = rowResult.get(PROTOCOL_STATUS).getValue();
    try {
      return (ProtocolStatus) Writables.getWritable(val, protocolStatus);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public float getScore() {
    return TableUtil.toFloat(rowResult.get(SCORE).getValue());
  }

  public byte[] getContent() {
    return rowResult.get(CONTENT).getValue();
  }

  public String getContentType() {
    return stringify(rowResult.get(CONTENT_TYPE));
  }

  public String getText() {
    return stringify(rowResult.get(TEXT));
  }

  public String getTitle() {
    return stringify(rowResult.get(TITLE));
  }

  public ParseStatus getParseStatus() {
    final ParseStatus parseStatus = new ParseStatus();
    final byte[] val = rowResult.get(PARSE_STATUS).getValue();
    try {
      return (ParseStatus) Writables.getWritable(val, parseStatus);
    } catch (final IOException e) {
      return null;
    }
  }

  public String getReprUrl() {
    return stringify(rowResult.get(REPR_URL));
  }

  public Collection<Outlink> getOutlinks() {
    final List<Outlink> outlinks = new ArrayList<Outlink>();
    for (final byte[] col : rowResult.keySet()) {
      final String column = Bytes.toString(col);
      if (column.startsWith(OUTLINKS_STR)) {
        final String toUrl = column.substring(OUTLINKS_STR_LEN);
        final String anchor = Bytes.toString(rowResult.get(col).getValue());
        outlinks.add(new Outlink(toUrl, anchor));
      }
    }
    return outlinks;
  }

  public Collection<Inlink> getInlinks() {
    final List<Inlink> inlinks = new ArrayList<Inlink>();
    for (final byte[] col : rowResult.keySet()) {
      final String column = Bytes.toString(col);
      if (column.startsWith(INLINKS_STR)) {
        final String fromUrl = column.substring(INLINKS_STR_LEN);
        final String anchor = Bytes.toString(rowResult.get(col).getValue());
        inlinks.add(new Inlink(fromUrl, anchor));
      }
    }

    return inlinks;
  }

  /** Returns a header.
   * @param key Header-key
   * @return headers if it exists, null otherwise
   */
  public String getHeader(String key) {
    final byte[] headerKey = Bytes.toBytes(HEADERS_STR  + key);
    if (!hasColumn(headerKey)) {
      return null;
    }
    return stringify(rowResult.get(headerKey));
  }

  /** Checks if a metadata key exists in "metadata" column.
   * @param row Row from hbase
   * @param metaKey Key to search in metadata column
   * @return true if key exists
   */
  public boolean hasMeta(String metaKey) {
    return hasColumn(Bytes.toBytes(METADATA_STR  + metaKey));
  }

  /** Read a metadata key from "metadata" column.
   * @param metaKey Key to search in metadata column
   * @return Value in byte array form or null if metadata doesn't exist
   */
  public byte[] getMeta(String metaKey) {
    final byte[] col = Bytes.toBytes(METADATA_STR + metaKey);
    return get(col);
  }

  /** Read a metadata key from "metadata" column.
   * @param metaKey Key to search in metadata column
   * @return Value in string form or null if metadata doesn't exist
   */
  public String getMetaAsString(String metaKey) {
    final byte[] val = getMeta(metaKey);
    return val == null ? null : Bytes.toString(val);
  }

}

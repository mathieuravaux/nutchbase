package org.apache.nutchbase.util.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.ProtocolStatus;

public class RowPart extends ImmutableRowPart {

  private final Map<byte[], byte[]> opMap =
    new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);

  /** For Writable. Do not use directly. */
  public RowPart() {
    super();
  }

  public RowPart(RowResult rowResult) {
    super(rowResult);
  }

  public RowPart(byte[] rowId) {
    super(rowId);
  }

  private static void checkForNull(Object o) {
    if (o == null) {
      // If null, the PUT becomes a DELETE operation.
      throw new IllegalArgumentException("Passed value cannot be null");
    }
  }

  // TODO: waay too slow. Find a faster approach
  private void deleteColumnAll(String colStart) {
    // first clear new additions/deletions
    final Iterator<Map.Entry<byte[], byte[]>> it = opMap.entrySet().iterator();
    while (it.hasNext()) {
      final byte[] key = it.next().getKey();
      final String keyString = Bytes.toString(key);
      if (keyString.startsWith(colStart)) {
        it.remove();
      }
    }

    // then add 'delete' operations for existing columns
    for (final byte[] col : rowResult.keySet()) {
      final String column = Bytes.toString(col);
      if (column.startsWith(colStart)) {
        opMap.put(col, null);
      }
    }
  }

  @Override
  public String getBaseUrl() {
    if (!opMap.containsKey(BASE_URL))
      return super.getBaseUrl();

    return Bytes.toString(opMap.get(BASE_URL));
  }

  public void setBaseUrl(String baseUrl) {
    checkForNull(baseUrl);
    opMap.put(BASE_URL, Bytes.toBytes(baseUrl));
  }

  @Override
  public byte[] getContent() {
    if (!opMap.containsKey(CONTENT))
      return super.getContent();

    return opMap.get(CONTENT);
  }

  public void setContent(byte[] content) {
    checkForNull(content);
    opMap.put(CONTENT, content);
  }

  @Override
  public String getContentType() {
    if (!opMap.containsKey(CONTENT_TYPE))
      return super.getContentType();

    return Bytes.toString(opMap.get(CONTENT_TYPE));
  }

  public void setContentType(String contentType) {
    checkForNull(contentType);
    opMap.put(CONTENT_TYPE, Bytes.toBytes(contentType));
  }

  @Override
  public int getFetchInterval() {
    if (!opMap.containsKey(FETCH_INTERVAL))
      return super.getFetchInterval();

    return Bytes.toInt(opMap.get(FETCH_INTERVAL));
  }

  public void setFetchInterval(int fetchInterval) {
    opMap.put(FETCH_INTERVAL, Bytes.toBytes(fetchInterval));
  }

  @Override
  public long getFetchTime() {
    if (!opMap.containsKey(FETCH_TIME))
      return super.getFetchTime();

    return Bytes.toLong(opMap.get(FETCH_TIME));
  }

  public void setFetchTime(long fetchTime) {
    opMap.put(FETCH_TIME, Bytes.toBytes(fetchTime));
  }


  @Override
  public long getPrevFetchTime() {
    if (!opMap.containsKey(PREV_FETCH_TIME))
      return super.getPrevFetchTime();

    final byte[] val = opMap.get(PREV_FETCH_TIME);
    if (val == null)
      return -1L;
    return Bytes.toLong(opMap.get(PREV_FETCH_TIME));
  }

  public void setPrevFetchTime(long prevFetchTime) {
    opMap.put(PREV_FETCH_TIME, Bytes.toBytes(prevFetchTime));
  }

  @Override
  public Collection<Outlink> getOutlinks() {
    final Collection<Outlink> outlinks = super.getOutlinks();
    final Map<String, Outlink> linkMap = new HashMap<String, Outlink>();

    for (final Outlink outlink : outlinks) {
      linkMap.put(outlink.getToUrl(), outlink);
    }

    for (final Map.Entry<byte[], byte[]> entry : opMap.entrySet()) {
      final String key = Bytes.toString(entry.getKey());
      if (key.startsWith(OUTLINKS_STR)) {
        final byte[] val = entry.getValue();
        if (val == null) { // outlink deleted
          linkMap.remove(key);
        } else { // new outlink
          final String toUrl = key.substring(OUTLINKS_STR_LEN);
          final String anchor = Bytes.toString(val);
          linkMap.put(key, new Outlink(toUrl, anchor));
        }
      }
    }
    return linkMap.values();
  }

  public void addOutlink(Outlink outlink) {
    final byte[] key = Bytes.toBytes(OUTLINKS_STR + outlink.getToUrl());
    opMap.put(key, Bytes.toBytes(outlink.getAnchor()));
  }

  public void deleteAllOutlinks() {
    deleteColumnAll(OUTLINKS_STR);
  }

  @Override
  public Collection<Inlink> getInlinks() {
    final Collection<Inlink> inlinks = super.getInlinks();
    final Map<String, Inlink> linkMap = new HashMap<String, Inlink>();

    for (final Inlink inlink : inlinks) {
      linkMap.put(inlink.getFromUrl(), inlink);
    }

    for (final Map.Entry<byte[], byte[]> entry : opMap.entrySet()) {
      final String key = Bytes.toString(entry.getKey());
      if (key.startsWith(INLINKS_STR)) {
        final byte[] val = entry.getValue();
        if (val == null) { // inlink deleted
          linkMap.remove(key);
        } else { // new outlink
          final String fromUrl = key.substring(INLINKS_STR_LEN);
          final String anchor = Bytes.toString(val);
          linkMap.put(key, new Inlink(fromUrl, anchor));
        }
      }
    }
    return linkMap.values();
  }

  public void addInlink(Inlink inlink) {
    checkForNull(inlink);
    final String fullKey = INLINKS_STR + inlink.getFromUrl();
    opMap.put(Bytes.toBytes(fullKey), Bytes.toBytes(inlink.getAnchor()));
  }

  public void deleteAllInlinks() {
    deleteColumnAll(INLINKS_STR);
  }

  @Override
  public ParseStatus getParseStatus() {
    if (!opMap.containsKey(PARSE_STATUS))
      return super.getParseStatus();

    final ParseStatus parseStatus = new ParseStatus();
    try {
      return (ParseStatus) Writables.getWritable(opMap.get(PARSE_STATUS),
                                                 parseStatus);
    } catch (final IOException e) {
      throw new RuntimeException(e); // TODO: really?
    }
  }

  public void setParseStatus(ParseStatus parseStatus) {
    checkForNull(parseStatus);
    try {
      opMap.put(PARSE_STATUS, Writables.getBytes(parseStatus));
    } catch (final IOException e) {
      throw new RuntimeException(e); // TODO: really?
    }
  }

  @Override
  public String getReprUrl() {
    if (!opMap.containsKey(REPR_URL))
      return super.getReprUrl();

    return Bytes.toString(opMap.get(REPR_URL));
  }

  public void setReprUrl(String reprUrl) {
    checkForNull(reprUrl);
    opMap.put(REPR_URL, Bytes.toBytes(reprUrl));
  }

  @Override
  public int getRetriesSinceFetch() {
    if (!opMap.containsKey(RETRIES))
      return super.getRetriesSinceFetch();

    return Bytes.toInt(opMap.get(RETRIES));
  }

  public void setRetriesSinceFetch(int retries) {
    opMap.put(RETRIES, Bytes.toBytes(retries));
  }

  @Override
  public ProtocolStatus getProtocolStatus() {
    if (!opMap.containsKey(PROTOCOL_STATUS))
      return super.getProtocolStatus();

    final ProtocolStatus protocolStatus = new ProtocolStatus();
    final byte[] val = opMap.get(PROTOCOL_STATUS);
    try {
      return (ProtocolStatus) Writables.getWritable(val, protocolStatus);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void setProtocolStatus(ProtocolStatus protocolStatus) {
    checkForNull(protocolStatus);
    try {
      opMap.put(PROTOCOL_STATUS,
                Writables.getBytes(protocolStatus));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float getScore() {
    if (!opMap.containsKey(SCORE))
      return super.getScore();

    return TableUtil.toFloat(opMap.get(SCORE));
  }

  public void setScore(float score) {
    opMap.put(SCORE, TableUtil.toBytes(score));
  }

  @Override
  public byte getStatus() {
    if (!opMap.containsKey(STATUS))
      return super.getStatus();

    return opMap.get(STATUS)[0];
  }

  public void setStatus(byte status) {
    opMap.put(STATUS, new byte[] { status });
  }

  @Override
  public byte[] getSignature() {
    if (!opMap.containsKey(SIGNATURE))
      return super.getSignature();

    return opMap.get(SIGNATURE);
  }

  public void setSignature(byte[] signature) {
    checkForNull(signature);
    opMap.put(SIGNATURE, signature);
  }

  @Override
  public byte[] getPrevSignature() {
    if (!opMap.containsKey(PREV_SIGNATURE))
      return super.getPrevSignature();

    return opMap.get(PREV_SIGNATURE);
  }

  public void setPrevSignature(byte[] prevSig) {
    checkForNull(prevSig);
    opMap.put(PREV_SIGNATURE, prevSig);
  }

  @Override
  public long getModifiedTime() {
    return Bytes.toLong(opMap.get(MODIFIED_TIME));
  }

  public void setModifiedTime(long modifiedTime) {
    opMap.put(MODIFIED_TIME, Bytes.toBytes(modifiedTime));
  }

  @Override
  public String getText() {
    if (!opMap.containsKey(TEXT))
      return super.getText();

    return Bytes.toString(opMap.get(TEXT));
  }

  public void setText(String text) {
    checkForNull(text);
    opMap.put(TEXT, Bytes.toBytes(text));
  }

  @Override
  public String getTitle() {
    if (!opMap.containsKey(TITLE))
      return super.getText();

    return Bytes.toString(opMap.get(TITLE));
  }

  public void setTitle(String title) {
    checkForNull(title);
    opMap.put(TITLE, Bytes.toBytes(title));
  }

  @Override
  public boolean hasColumn(byte[] col) {
    if (!opMap.containsKey(col))
      return super.hasColumn(col);

    return opMap.get(col) != null; // check if column is deleted
  }

  public void putColumn(byte[] key, byte[] val) {
      opMap.put(key, val);
  }

  public void putColumn(String key, String val) {
      opMap.put(Bytes.toBytes(key), Bytes.toBytes(val));
  }

  public void deleteColumn(String key) {
      opMap.put(Bytes.toBytes(key), null);
  }

  @Override
  public boolean hasMeta(String metaKey) {
    return hasColumn(Bytes.toBytes(METADATA_STR + metaKey));
  }

  @Override
  public String getHeader(String key) {
    final byte[] headerKey = Bytes.toBytes(HEADERS_STR  + key);
    if (opMap.containsKey(headerKey)) {
      final byte[] val = opMap.get(headerKey);
      if (val == null) { // deleted !!!
        return null;
      }
      return Bytes.toString(val);
    }
    return stringify(rowResult.get(headerKey));
  }

  public void addHeader(String key, String value) {
    checkForNull(value);
    opMap.put(Bytes.toBytes(HEADERS_STR + key),
              Bytes.toBytes(value));
  }

  public void deleteHeaders() {
    deleteColumnAll(HEADERS_STR);
  }

  @Override
  public byte[] getColumn(String key) {
    byte[] bKey = Bytes.toBytes(key);
    if (opMap.containsKey(bKey))
      return opMap.get(bKey);
    return super.getColumn(key);
  }

  @Override
  public Set<byte[]> getColumns() {
    Set<byte[]> columns = new HashSet<byte[]>(super.getColumns());
    columns.addAll(opMap.keySet());
    return columns;
  }

  @Override
  public byte[] getMeta(String metaKey) {
    final String fullKeyString = METADATA_STR + metaKey;
    final byte[] key = Bytes.toBytes(fullKeyString);
    if (!opMap.containsKey(key))
      return super.get(key);
    return opMap.get(key);
  }

  @Override
  public String getMetaAsString(String metaKey) {
    final byte[] val = getMeta(metaKey);
    return val == null ? null : Bytes.toString(val);
  }

  public void putMeta(String metaKey, byte[] val) {
    checkForNull(val);
    opMap.put(Bytes.toBytes(METADATA_STR + metaKey), val);
  }

  public void deleteMeta(String metaKey) {
    opMap.put(Bytes.toBytes(METADATA_STR + metaKey), null);
  }

  public BatchUpdate makeBatchUpdate() {
    final BatchUpdate bu = new BatchUpdate(getRowId(), System.currentTimeMillis());

    for (final Map.Entry<byte[], byte[]> entry : opMap.entrySet()) {
      final byte[] val = entry.getValue();
      if (val == null) { // delete op
        bu.delete(entry.getKey());
      } else { // put op
        bu.put(entry.getKey(), val);
      }
    }

    return bu;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    opMap.clear();
    final int size = in.readInt();
    for (int i = 0; i < size; i++) {
      final byte[] key = Bytes.readByteArray(in);

      byte[] val = null;
      if (in.readBoolean()) {
        val = Bytes.readByteArray(in);
      }

      opMap.put(key, val);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(opMap.size());
    for (final Map.Entry<byte[], byte[]> op : opMap.entrySet()) {
      Bytes.writeByteArray(out, op.getKey());

      final byte[] val = op.getValue();
      if (val == null) { // a delete operation
        out.writeBoolean(false);
      } else { // a put operation
        out.writeBoolean(true);
        Bytes.writeByteArray(out, val);
      }
    }
  }

}

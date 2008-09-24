package org.apache.nutchbase.util.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.protocol.ProtocolStatus;

public class RowPart extends ImmutableRowPart {

  private Map<byte[], byte[]> opMap =
    new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
  
  public RowPart() { }

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
    Iterator<Map.Entry<byte[], byte[]>> it = opMap.entrySet().iterator();
    while (it.hasNext()) {
      byte[] key = it.next().getKey();
      String keyString = Bytes.toString(key);
      if (keyString.startsWith(colStart)) {
        it.remove();
      }
    }
    
    // then add 'delete' operations for existing columns
    for (byte[] col : rowResult.keySet()) {
      String column = Bytes.toString(col);
      if (column.startsWith(colStart)) {
        opMap.put(col, null);
      }
    }
  }
  
  @Override
  public String getBaseUrl() {
    if (!opMap.containsKey(TableColumns.BASE_URL))
      return super.getBaseUrl();
    
    return Bytes.toString(opMap.get(TableColumns.BASE_URL));
  }
  
  public void setBaseUrl(String baseUrl) {
    checkForNull(baseUrl);
    opMap.put(TableColumns.BASE_URL, Bytes.toBytes(baseUrl));
  }

  @Override
  public byte[] getContent() {
    if (!opMap.containsKey(TableColumns.CONTENT))
      return super.getContent();
    
    return opMap.get(TableColumns.BASE_URL);
  }
  
  public void setContent(byte[] content) {
    checkForNull(content);
    opMap.put(TableColumns.CONTENT, content);
  }

  @Override
  public String getContentType() {
    if (!opMap.containsKey(TableColumns.CONTENT_TYPE))
      return super.getContentType();
    
    return Bytes.toString(opMap.get(TableColumns.CONTENT_TYPE));
  }
  
  public void setContentType(String contentType) {
    checkForNull(contentType);
    opMap.put(TableColumns.CONTENT_TYPE, Bytes.toBytes(contentType));
  }

  @Override
  public int getFetchInterval() {
    if (!opMap.containsKey(TableColumns.FETCH_INTERVAL))
      return super.getFetchInterval();
 
    return Bytes.toInt(opMap.get(TableColumns.FETCH_INTERVAL)); 
  }
  
  public void setFetchInterval(int fetchInterval) {
    opMap.put(TableColumns.FETCH_INTERVAL, Bytes.toBytes(fetchInterval));
  }

  @Override
  public long getFetchTime() {
    if (!opMap.containsKey(TableColumns.FETCH_TIME))
      return super.getFetchTime();
    
    return Bytes.toLong(opMap.get(TableColumns.FETCH_TIME));
  }
  
  public void setFetchTime(long fetchTime) {
    opMap.put(TableColumns.FETCH_TIME, Bytes.toBytes(fetchTime));
  }

  @Override
  public Collection<Outlink> getOutlinks() {    
    Collection<Outlink> outlinks = super.getOutlinks();
    Map<String, Outlink> linkMap = new HashMap<String, Outlink>();
    
    for (Outlink outlink : outlinks) {
      linkMap.put(outlink.getToUrl(), outlink);
    }
    
    for (Map.Entry<byte[], byte[]> entry : opMap.entrySet()) {
      String key = Bytes.toString(entry.getKey());
      if (key.startsWith(TableColumns.OUTLINKS_STR)) {
        byte[] val = entry.getValue();
        if (val == null) { // outlink deleted
          linkMap.remove(key);
        } else { // new outlink
          String toUrl = key.substring(OUTLINKS_STR_LEN);
          String anchor = Bytes.toString(val);
          linkMap.put(key, new Outlink(toUrl, anchor));
        }
      }
    }
    return linkMap.values();
  }
  
  public void addOutlink(Outlink outlink) {
    byte[] key = Bytes.toBytes(TableColumns.OUTLINKS_STR + outlink.getToUrl());
    opMap.put(key, Bytes.toBytes(outlink.getAnchor()));
  }
  
  public void deleteAllOutlinks() {
    deleteColumnAll(TableColumns.OUTLINKS_STR);
  }
  
  @Override
  public Collection<Inlink> getInlinks() {
    Collection<Inlink> inlinks = super.getInlinks();
    Map<String, Inlink> linkMap = new HashMap<String, Inlink>();
    
    for (Inlink inlink : inlinks) {
      linkMap.put(inlink.getFromUrl(), inlink);
    }

    for (Map.Entry<byte[], byte[]> entry : opMap.entrySet()) {
      String key = Bytes.toString(entry.getKey());
      if (key.startsWith(TableColumns.INLINKS_STR)) {
        byte[] val = entry.getValue();
        if (val == null) { // inlink deleted
          linkMap.remove(key);
        } else { // new outlink
          String fromUrl = key.substring(INLINKS_STR_LEN);
          String anchor = Bytes.toString(val);
          linkMap.put(key, new Inlink(fromUrl, anchor));
        }
      }
    }
    return linkMap.values();
  }
  
  public void addInlink(Inlink inlink) {
    checkForNull(inlink);
    String fullKey = TableColumns.INLINKS_STR + inlink.getFromUrl();
    opMap.put(Bytes.toBytes(fullKey), Bytes.toBytes(inlink.getAnchor()));
  }
  
  public void deleteAllInlinks() {
    deleteColumnAll(TableColumns.INLINKS_STR);
  }

  @Override
  public ParseStatus getParseStatus() {
    if (!opMap.containsKey(TableColumns.PARSE_STATUS))
      return super.getParseStatus();
    
    ParseStatus parseStatus = new ParseStatus();
    try {
      return (ParseStatus) Writables.getWritable(opMap.get(TableColumns.PARSE_STATUS),
                                                 parseStatus);
    } catch (IOException e) {
      throw new RuntimeException(e); // TODO: really?
    }
  }
  
  public void setParseStatus(ParseStatus parseStatus) {
    checkForNull(parseStatus);
    try {
      opMap.put(TableColumns.PARSE_STATUS, Writables.getBytes(parseStatus));
    } catch (IOException e) {
      throw new RuntimeException(e); // TODO: really?
    }
  }

  @Override
  public String getReprUrl() {
    if (!opMap.containsKey(TableColumns.REPR_URL))
      return super.getReprUrl();
    
    return Bytes.toString(opMap.get(TableColumns.REPR_URL));
  }
  
  public void setReprUrl(String reprUrl) {
    checkForNull(reprUrl);
    opMap.put(TableColumns.REPR_URL, Bytes.toBytes(reprUrl));
  }

  @Override
  public int getRetriesSinceFetch() {
    if (!opMap.containsKey(TableColumns.RETRIES))
      return super.getRetriesSinceFetch();
    
    return Bytes.toInt(opMap.get(TableColumns.RETRIES));
  }
  
  public void setRetriesSinceFetch(int retries) {
    opMap.put(TableColumns.RETRIES, Bytes.toBytes(retries));
  }
  
  @Override
  public ProtocolStatus getProtocolStatus() {
    if (!opMap.containsKey(TableColumns.PROTOCOL_STATUS))
      return super.getProtocolStatus();

    ProtocolStatus protocolStatus = new ProtocolStatus();
    byte[] val = opMap.get(TableColumns.PROTOCOL_STATUS);
    try {
      return (ProtocolStatus) Writables.getWritable(val, protocolStatus);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void setProtocolStatus(ProtocolStatus protocolStatus) {
    checkForNull(protocolStatus);
    try {
      opMap.put(TableColumns.PROTOCOL_STATUS,
                Writables.getBytes(protocolStatus));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float getScore() {
    if (!opMap.containsKey(TableColumns.SCORE))
      return super.getScore();
    
    return TableUtil.toFloat(opMap.get(TableColumns.SCORE));
  }
  
  public void setScore(float score) {
    opMap.put(TableColumns.SCORE, TableUtil.toBytes(score));
  }

  @Override
  public byte getStatus() {
    if (!opMap.containsKey(TableColumns.STATUS))
      return super.getStatus();
    
    return opMap.get(TableColumns.STATUS)[0];
  }
  
  public void setStatus(byte status) {
    opMap.put(TableColumns.STATUS, new byte[] { status });
  }
  
  @Override
  public byte[] getSignature() {
    if (!opMap.containsKey(TableColumns.SIGNATURE))
      return super.getSignature();
    
    return opMap.get(TableColumns.SIGNATURE);
  }
  
  public void setSignature(byte[] signature) {
    checkForNull(signature);
    opMap.put(TableColumns.SIGNATURE, signature);
  }
  
  @Override
  public long getModifiedTime() {
    return Bytes.toLong(opMap.get(TableColumns.MODIFIED_TIME));
  }

  public void setModifiedTime(long modifiedTime) {
    opMap.put(TableColumns.MODIFIED_TIME, Bytes.toBytes(modifiedTime));
  }
  
  @Override
  public String getText() {
    if (!opMap.containsKey(TableColumns.TEXT))
      return super.getText();
    
    return Bytes.toString(opMap.get(TableColumns.TEXT));
  }
    
  public void setText(String text) {
    checkForNull(text);
    opMap.put(TableColumns.TEXT, Bytes.toBytes(text));
  }
  
  @Override
  public String getTitle() {
    if (!opMap.containsKey(TableColumns.TITLE))
      return super.getText();
    
    return Bytes.toString(opMap.get(TableColumns.TITLE));
  }
  
  public void setTitle(String title) {
    checkForNull(title);
    opMap.put(TableColumns.TITLE, Bytes.toBytes(title));
  }

  @Override
  public boolean hasColumn(byte[] col) {
    if (!opMap.containsKey(col))
      return super.hasColumn(col);

    return opMap.get(col) != null; // check if column is deleted
  }

  @Override
  public boolean hasMeta(String metaKey) {
    return hasColumn(Bytes.toBytes(TableColumns.METADATA_STR + metaKey));
  }
  
  @Override
  public byte[] getMeta(String metaKey) {
    String fullKeyString = TableColumns.METADATA_STR + metaKey;
    byte[] key = Bytes.toBytes(fullKeyString);
    if (!opMap.containsKey(key))
      return rowResult.get(key).getValue();
    
    return opMap.get(key);
  }
  
  public void putMeta(String metaKey, byte[] val) {
    checkForNull(val);
    opMap.put(Bytes.toBytes(TableColumns.METADATA_STR + metaKey), val);
  }
  
  public void deleteMeta(String metaKey) {
    opMap.put(Bytes.toBytes(TableColumns.METADATA_STR + metaKey), null);
  }
  
  public BatchUpdate makeBatchUpdate(byte[] row) {
    BatchUpdate bu = new BatchUpdate(row);
    
    for (Map.Entry<byte[], byte[]> entry : opMap.entrySet()) {
      byte[] val = entry.getValue();
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
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      byte[] key = Bytes.readByteArray(in);
      
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
    for (Map.Entry<byte[], byte[]> op : opMap.entrySet()) {
      Bytes.writeByteArray(out, op.getKey());
      
      byte[] val = op.getValue();
      if (val == null) { // a delete operation
        out.writeBoolean(false);
      } else { // a put operation
        out.writeBoolean(true);
        Bytes.writeByteArray(out, val);
      }
    }
  }

}

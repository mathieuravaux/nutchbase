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

package org.apache.nutchbase.crawl;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.MD5Hash;

import org.apache.nutchbase.parse.ParseHbase;
import org.apache.nutchbase.util.hbase.ImmutableRowPart;
import org.apache.nutchbase.util.hbase.TableColumns;

/**
 * <p>An implementation of a page signature. It calculates an MD5 hash
 * of a plain text "profile" of a page. In case there is no text, it
 * calculates a hash using the {@link MD5Signature}.</p>
 * <p>The algorithm to calculate a page "profile" takes the plain text version of
 * a page and performs the following steps:
 * <ul>
 * <li>remove all characters except letters and digits, and bring all characters
 * to lower case,</li>
 * <li>split the text into tokens (all consecutive non-whitespace characters),</li>
 * <li>discard tokens equal or shorter than MIN_TOKEN_LEN (default 2 characters),</li>
 * <li>sort the list of tokens by decreasing frequency,</li>
 * <li>round down the counts of tokens to the nearest multiple of QUANT
 * (<code>QUANT = QUANT_RATE * maxFreq</code>, where <code>QUANT_RATE</code> is 0.01f
 * by default, and <code>maxFreq</code> is the maximum token frequency). If
 * <code>maxFreq</code> is higher than 1, then QUANT is always higher than 2 (which
 * means that tokens with frequency 1 are always discarded).</li>
 * <li>tokens, which frequency after quantization falls below QUANT, are discarded.</li>
 * <li>create a list of tokens and their quantized frequency, separated by spaces,
 * in the order of decreasing frequency.</li>
 * </ul>
 * This list is then submitted to an MD5 hash calculation.
 * 
 * @author Andrzej Bialecki &lt;ab@getopt.org&gt;
 */
public class TextProfileSignatureHbase extends SignatureHbase {

  private final static Set<String> COLUMNS = new HashSet<String>();
  
  static {
    COLUMNS.add(TableColumns.CONTENT_STR);
  }
  
  SignatureHbase fallback = new MD5SignatureHbase();

  public byte[] calculate(ImmutableRowPart row, ParseHbase parse) {
    int MIN_TOKEN_LEN = getConf().getInt("db.signature.text_profile.min_token_len", 2);
    float QUANT_RATE = getConf().getFloat("db.signature.text_profile.quant_rate", 0.01f);
    HashMap<String, Token> tokens = new HashMap<String, Token>();
    String text = null;
    if (parse != null) text = parse.getText();
    if (text == null || text.length() == 0) return fallback.calculate(row, parse);
    StringBuffer curToken = new StringBuffer();
    int maxFreq = 0;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isLetterOrDigit(c)) {
        curToken.append(Character.toLowerCase(c));
      } else {
        if (curToken.length() > 0) {
          if (curToken.length() > MIN_TOKEN_LEN) {
            // add it
            String s = curToken.toString();
            Token tok = tokens.get(s);
            if (tok == null) {
              tok = new Token(0, s);
              tokens.put(s, tok);
            }
            tok.cnt++;
            if (tok.cnt > maxFreq) maxFreq = tok.cnt;
          }
          curToken.setLength(0);
        }
      }
    }
    // check the last token
    if (curToken.length() > MIN_TOKEN_LEN) {
      // add it
      String s = curToken.toString();
      Token tok = tokens.get(s);
      if (tok == null) {
        tok = new Token(0, s);
        tokens.put(s, tok);
      }
      tok.cnt++;
      if (tok.cnt > maxFreq) maxFreq = tok.cnt;
    }
    Iterator<Token> it = tokens.values().iterator();
    ArrayList<Token> profile = new ArrayList<Token>();
    // calculate the QUANT value
    int QUANT = Math.round(maxFreq * QUANT_RATE);
    if (QUANT < 2) {
      if (maxFreq > 1) QUANT = 2;
      else QUANT = 1;
    }
    while(it.hasNext()) {
      Token t = it.next();
      // round down to the nearest QUANT
      t.cnt = (t.cnt / QUANT) * QUANT;
      // discard the frequencies below the QUANT
      if (t.cnt < QUANT) {
        continue;
      }
      profile.add(t);
    }
    Collections.sort(profile, new TokenComparator());
    StringBuffer newText = new StringBuffer();
    it = profile.iterator();
    while (it.hasNext()) {
      Token t = it.next();
      if (newText.length() > 0) newText.append("\n");
      newText.append(t.toString());
    }
    return MD5Hash.digest(newText.toString()).getDigest();
  }

  public Set<String> getColumnSet() {
    return COLUMNS;
  }
  
  private static class Token {
    public int cnt;
    public String val;
    
    public Token(int cnt, String val) {
      this.cnt = cnt;
      this.val = val;
    }
    
    public String toString() {
      return val + " " + cnt;
    }
  }
  
  private static class TokenComparator implements Comparator<Token> {
    public int compare(Token t1, Token t2) {
      return t2.cnt - t1.cnt;
    }
  }
}

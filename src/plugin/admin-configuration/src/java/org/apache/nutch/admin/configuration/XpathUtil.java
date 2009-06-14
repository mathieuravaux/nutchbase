/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.admin.configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.xpath.XPathAPI;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XpathUtil {

  public static void saveConfiguration(Map parameterMap,
      Configuration configuration, OutputStream outputStream)
      throws ParserConfigurationException, TransformerException, IOException {
    
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    Document document = factory.newDocumentBuilder().newDocument();
    Element configurationElement = document.createElement("configuration");
    Node node = document.appendChild(configurationElement);
    Set set = parameterMap.keySet();
    TreeSet treeSet = new TreeSet(set);
    treeSet.remove("action");
    treeSet.remove("config");
    for (Iterator iter = treeSet.iterator(); iter.hasNext();) {
      // e.g. key=http.agent.name
      String key = (String) iter.next();
      // e.g. strings=Nutch
      String[] strings = (String[]) parameterMap.get(key);
      String valueFromMap = strings[0];
      int index = key.indexOf(" ");
      if (index > -1) {
        key = key.substring(index + 1);
      }
      //
      configuration.set(key, valueFromMap);
      Element property = document.createElement("property");
      Element name = document.createElement("name");
      name.appendChild(document.createTextNode(key));
      Element value = document.createElement("value");
      value.appendChild(document.createTextNode(valueFromMap));
      Element description = document.createElement("description");
      Node propertyNode = node.appendChild(property);
      propertyNode.appendChild(name);
      propertyNode.appendChild(value);
      propertyNode.appendChild(description);
    }

    save(document, outputStream);
  }


  public static void saveConfiguration(Document document, 
      Node newNode, OutputStream outputStream) 
      throws TransformerException, IOException {

    boolean duplicate = checkDuplicateNodes(document, newNode);
    if (!duplicate) {
      Node node = document.importNode(newNode, true);
      Node firstChild = document.getDocumentElement();
      firstChild.appendChild(node);
    }
    save(document, outputStream);
  }

  
  private static void save(Document document, OutputStream outputStream)
      throws TransformerFactoryConfigurationError,
      TransformerConfigurationException, TransformerException, IOException {
    DOMSource source = new DOMSource(document);
    StreamResult result = new StreamResult(outputStream);
    TransformerFactory transFactory = TransformerFactory.newInstance();
    Transformer transformer = transFactory.newTransformer();
    transformer.setOutputProperty("indent", "yes");
    transformer.transform(source, result);
    outputStream.close();
  }

  /**
   * @param document
   * @param newNode
   * @return true if an property with the same name exists in the document
   * @throws TransformerException
   */
  
  private static boolean checkDuplicateNodes(final Document document,
      final Node newNode) 
      throws TransformerException {
    
    boolean bit = false;
    Element element = document.getDocumentElement();
    Node newProperty = XPathAPI.selectSingleNode(newNode, "name");
    NodeList list = XPathAPI.selectNodeList(element, "//name");
    for (int i = 0; i < list.getLength(); i++) {
      Node actualProperty = list.item(i);
      if (newProperty.getFirstChild().getNodeValue().equals(
          actualProperty.getFirstChild().getNodeValue())) {
        bit = true;
      }
    }
    return bit;
  }
}
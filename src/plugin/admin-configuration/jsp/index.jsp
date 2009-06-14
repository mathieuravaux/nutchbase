<%@ page
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
%>


<%@ page import="org.w3c.dom.Document" %>
<%@ page import="org.xml.sax.SAXException" %>
<%@ page import="javax.xml.parsers.DocumentBuilderFactory" %>
<%@ page import="javax.xml.parsers.ParserConfigurationException" %>
<%@ page import="org.apache.xpath.XPathAPI" %>
<%@ page import="org.w3c.dom.Document" %>
<%@ page import="org.w3c.dom.Element" %>
<%@ page import="org.w3c.dom.Node" %>
<%@ page import="org.w3c.dom.NodeList" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.Iterator" %>
<%@ page import="java.util.Set" %>
<%@ page import="org.apache.nutch.admin.configuration.XpathUtil" %>
<%@ page import="org.apache.nutch.admin.*" %>
<%@ page import="java.io.File" %>
<%@ page import="java.util.Enumeration" %>
<%@ page import="java.text.DecimalFormat" %>
<%@ page import="org.apache.hadoop.fs.FileSystem" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.hadoop.fs.*" %>
<%@ page import="org.apache.nutch.util.NutchConfiguration" %>
<%@ page import="java.io.FileOutputStream" %>


<%
    GuiComponent component = (GuiComponent) application.getAttribute("component");
    Configuration configuration = component.getNutchInstance().getConfiguration();
    Path instanceFolder = component.getNutchInstance().getInstanceFolder();
    FileSystem fs = FileSystem.get(configuration);
    
    LocalFileSystem localFS = new LocalFileSystem(fs);
    
    String error = request.getParameter("error");
    String selectedNutchProperty = request.getParameter("nutchdropdown")!=null ? request.getParameter("nutchdropdown") : "";

    Path nutchSiteFile = new Path(instanceFolder, "conf/nutch-site.xml");
    Path nutchDefaultFile = new Path(instanceFolder, "conf/nutch-default.xml");
    NodeList nutchSiteProperties = null;
    NodeList nutchDefaultProperties = null;
    
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    
    //save configuration
    if("save".equals(request.getParameter("action"))) {
    	 Map parameterMap = request.getParameterMap();
    	 String filename = nutchSiteFile.toString().replace("file:", "");
         //System.out.println("Filename : " + filename);
         XpathUtil.saveConfiguration(parameterMap, configuration, new FileOutputStream(filename));
    }

    Document nutchSiteDocument = factory.newDocumentBuilder().parse(nutchSiteFile.toString());
    Document nutchDefaultDocument = factory.newDocumentBuilder().parse(nutchDefaultFile.toString());
    nutchSiteProperties = nutchSiteDocument.getElementsByTagName("property");
    nutchDefaultProperties = nutchDefaultDocument.getElementsByTagName("property");
%>
	
<html>
<head>
<title>Nutch Administration</title>
<link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<div align="center"> <font class="headline"><%=component.getLabel("configuration.headline0", request.getLocale())%></font> <a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(<%=component.getLabel("configuration.help", request.getLocale())%>)</font></a><br>
    <br>
</div>

<br>
<hr/>
<form method="post" action="index.jsp">
<div align="left">
<select name="nutchdropdown">
<%
for (int i = 0; i < nutchDefaultProperties.getLength(); i++) {
      Node property = nutchDefaultProperties.item(i);
      Node name = XPathAPI.selectSingleNode(property, "name");
      //if a property are submitted then save it in the nutch-site.xml
      if(selectedNutchProperty.equals(name.getFirstChild().getNodeValue())) {
      	XpathUtil.saveConfiguration(nutchSiteDocument, property, new FileOutputStream(
            nutchSiteFile.toString()));
      	nutchSiteDocument = factory.newDocumentBuilder().parse(nutchSiteFile.toString());
      }
      %>
      <option value="<%=name.getFirstChild().getNodeValue()%>"><%=name.getFirstChild().getNodeValue()%></option>
<%
}
%>
</select>
<input type="submit" value="<%=component.getLabel("configuration.overwrite", request.getLocale())%>"/>
<input type="hidden" value="add-nutch-property"/>
</div>
</form>

<hr/>
<div>
<font class="headline"><%=component.getLabel("configuration.headline1", request.getLocale())%></font><br/>
<font class="notice"><%=nutchSiteFile.toString()%></font>
</div>
<form method="post" action="index.jsp">
  <table width="90%" class="table" align="center">
    <tr> 
      <td class="tableHead" width="150"><%=component.getLabel("configuration.property", request.getLocale())%></td>
      <td class="tableHead" width="200"><%=component.getLabel("configuration.value", request.getLocale())%></td>
    </tr>
    <%
    
      for (int i = 0; i < nutchSiteProperties.getLength(); i++) {
      Node property = nutchSiteProperties.item(i);
      Node name = XPathAPI.selectSingleNode(property, "name");
      Node value = XPathAPI.selectSingleNode(property, "value");
      String color = "tableCellWhite";
      if(i%2 == 0) {
      	color = "tableCellGrey";
      }
    %>
      <tr> 
      <td class="<%=color%>"><%=name.getFirstChild().getNodeValue()%></td>
      <td class="<%=color%>">
        <input type="text" name="<%=new DecimalFormat("000").format(i)%> <%=name.getFirstChild().getNodeValue()%>" value="<%=value.getFirstChild().getNodeValue()%>" style="width:100%"/>
        
      </td>
    </tr>
          <%
    }
    
    %>
    
	    <tr>
      <td class="tableCellWhite">&nbsp;</td>
      <td class="tableCellWhite" colspan="3">
      	<input type="hidden" name="action" value="save"/>
      	<input type="submit" value="<%=component.getLabel("configuration.save", request.getLocale())%>"/>
      </td>
     </tr>
  </table>
</form>
</body>
</html>

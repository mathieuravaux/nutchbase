<%@ page
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
%>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.nutch.util.NutchConfiguration" %>
<%@ page import="org.apache.nutch.crawl.Injector" %>
<%@ page import="org.apache.hadoop.fs.Path" %>
<%@ page import="java.lang.Thread" %>
<%@ page import="java.lang.Runnable" %>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="org.apache.nutch.admin.*" %>
<%@ page import="org.apache.nutch.admin.inject.*" %>
<%@ page import="org.apache.hadoop.fs.FileSystem" %>





<%	
String message = "empty_label";
GuiComponent component = (GuiComponent) application.getAttribute("component");
Configuration configuration = component.getNutchInstance().getConfiguration();
Path instanceFolder = component.getNutchInstance().getInstanceFolder();
FileSystem fileSystem = FileSystem.get(configuration);
String urls = request.getParameter("urls");
if(urls != null && !"".equals(urls.trim())) {
	// write this in a temporary file
    // TODO !!!
    Path urlfolder = new Path(instanceFolder, "temp_inject_" + System.currentTimeMillis());
    Path seed = new Path(urlfolder, "seed");
    
    FSDataOutputStream output = fileSystem.create(seed);
    PrintWriter pw = new PrintWriter(output.getWrappedStream());
    pw.write(urls);
    pw.close();
    output.close();
    
    System.out.println("Written seed file to " + seed);
    
	InjectThread thread = new InjectThread(configuration, urlfolder);
	thread.start();
	//wait for the message from the thread
	Thread.sleep(500);
	message = thread.getMessage();
}
%>

<%@page import="javax.xml.crypto.Data"%>
<%@page import="java.util.Date"%>
<%@page import="java.io.File"%>
<%@page import="org.apache.hadoop.fs.FSDataOutputStream"%>
<%@page import="java.io.OutputStream"%>
<%@page import="java.io.BufferedOutputStream"%>
<%@page import="java.io.PrintWriter"%><html>
<head>
<title>Nutch Url Injection</title>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
<link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<div style="color:green"><%=component.getLabel(message, request.getLocale())%></div>
<div align="center"> <font class="headline"><%=component.getLabel("inject.headline0", request.getLocale())%></font> <a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(<%=component.getLabel("inject.help", request.getLocale())%>)</font></a><br>
    <br>
</div>
<div> <font class="headline"><%=component.getLabel("inject.headline1", request.getLocale())%></font></div>
<form method="post" action="index.jsp">
<table width="90%" border="0" class="table" align="center">
  <tr> 
      <td colspan="1" class="tableHead"><%=component.getLabel("url.path.long", request.getLocale())%></td>
  </tr>
  <tr> 
    <td>
        <textarea name="urls" rows="30" style="width:100%;"></textarea>
    </td>
  </tr>
  <tr> 
    <td class="tableCellWhite" align="center"><input type="submit" value="<%=component.getLabel("inject.button", request.getLocale())%>"/></td>
  </tr>
</table>
</form>

</body>
</html>

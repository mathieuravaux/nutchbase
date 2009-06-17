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
<%@ page import="org.apache.nutch.admin.index.*" %>
<%@ page import="org.apache.hadoop.fs.FileSystem" %>





<%	
GuiComponent component = (GuiComponent) application.getAttribute("component");
Configuration configuration = component.getNutchInstance().getConfiguration();
Path instanceFolder = component.getNutchInstance().getInstanceFolder();
FileSystem fileSystem = FileSystem.get(configuration);

String reindex = request.getParameter("reindex");
if(reindex != null && !"".equals(reindex.trim())) {
    System.out.println("Launching a re-indexation...");
    IndexThread thread = new IndexThread(configuration);
    thread.start();
    //wait for the message from the thread
    Thread.sleep(500);
    // message = thread.getMessage();
}
%>

<head>
  <title>Nutch Index management</title>
  <link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>
<body bgcolor="#FFFFFF" text="#000000">
  <div align="center">
    <font class="headline">Nutch Index Management</font>
    <a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(Aide)</font></a>
    <br>
  </div>


<form method="post" action="index.jsp">
   <input type="hidden" name="reindex" value="true" />
   <input type="submit" value="Re-generer l'index"/>
</form>

</body>
</html>

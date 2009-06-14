<%@ page
  contentType="text/html; charset=utf-8"
  pageEncoding="utf-8"
%>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="java.io.FileFilter" %>
<%@ page import="java.util.Arrays" %>
<%@ page import="java.util.Set" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.HashSet" %>
<%@ page import="java.util.Iterator" %>
<%@ page import="org.apache.nutch.admin.management.*" %>
<%@ page import="org.apache.nutch.admin.TaskThread" %>
<%@ page import="org.apache.hadoop.fs.Path" %>
<%@ page import="org.apache.hadoop.fs.FileSystem" %>
<%@ page import="org.apache.nutch.admin.GuiComponent" %>

<%
String refresh = "on".equals(request.getParameter("refresh")) ? "on" : "off";
String isRefresh = "off".equals(refresh) ? "on" : "off";
String message = "empty_label";
String disableGenerate = "disabled=\"disabled\"";
String parameter = request.getParameter("action");
String error = request.getParameter("error");
GuiComponent component = (GuiComponent) application.getAttribute("component");
Configuration configuration = component.getNutchInstance().getConfiguration();
Path instanceFolder = component.getNutchInstance().getInstanceFolder();
FileSystem fileSystem = FileSystem.get(configuration);

Path crawlDir = new Path(configuration.get("crawl.dir"));

    TaskThread thread = null;
    if("generate".equals(parameter)) {
        String fetcher = request.getParameter("fetcher") != null ? request.getParameter("fetcher") : "-1";
        String days = request.getParameter("days") != null ? request.getParameter("days") : "0";
        String topn = request.getParameter("topn") != null ? request.getParameter("topn") : "1000000";
        thread = new GenerateThread(configuration, new Path(crawlDir, "crawldb"), new Path(crawlDir, "segments"), topn, fetcher, days);
    }else if("fetch".equals(parameter)) {
        thread = new FetchThread(configuration, new Path(request.getParameter("segment")));
    }else if("index".equals(parameter)) {
        Path file = new Path(request.getParameter("segment"));
        thread = new IndexThread(configuration, new Path(crawlDir,"segments/"+file.getName()+"/index"), new Path(crawlDir,"crawldb"), new Path(crawlDir,"linkdb"), new Path[]{file});
    }else if("parse".equals(parameter)) {
        Path file = new Path(request.getParameter("segment"));
        thread = new ParseThread(configuration, file);
    }else if("updatecrawldb".equals(parameter)) {
        thread = new CrawldbThread(configuration, new Path(crawlDir,"crawldb"), new Path(request.getParameter("segment")));
    }else if("invert".equals(parameter)) {
        Path[] files = new Path[]{new Path(request.getParameter("segment"))};
        thread = new LinkdbThread(configuration, new Path(crawlDir,"linkdb"), files);
    }else if("deletesegment".equals(parameter)) {
        thread = new DeleteFileThread(configuration,new Path[]{new Path(request.getParameter("segment"))}); 
    }else if("deleteindex".equals(parameter)) {
        thread = new DeleteFileThread(configuration,new Path[]{new Path(request.getParameter("segment"), "index")}); 
    }else if("deleteparse".equals(parameter)) {
        thread = new DeleteFileThread(configuration,new Path[]{new Path(request.getParameter("segment"), "crawl_parse"), new Path(request.getParameter("segment"), "parse_text"), new Path(request.getParameter("segment"), "parse_data"), new Path(request.getParameter("segment"), "parse.done"), new Path(request.getParameter("segment"), "invert.done")}); 
    }else if("putsearch".equals(parameter)) {
        thread = new CreateFileThread(configuration, new Path(request.getParameter("segment"), "search.done")); 
    }else if("removesearch".equals(parameter)) {
        thread = new DeleteFileThread(configuration, new Path[]{new Path(request.getParameter("segment"), "search.done")}); 
    }
    
    if(thread != null) {
        thread.start();
        //wait for the message from the thread
        Thread.sleep(500);
        message = thread.getMessage();
    }
    
    
%>    
<html>
<head>
<title>Nutch Administration</title>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
<%if("on".equals(refresh)) {%><meta http-equiv="refresh" content="10"><%}%>
<link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<div style="color:green"><%=component.getLabel(message, request.getLocale())%></div>
<div align="center"> <font class="headline">Management</font> <a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(<%=component.getLabel("management.help", request.getLocale())%>)</font></a><br>
    <br>
</div>

<br/>

<div align="center"><a href="?refresh=<%=isRefresh%>"><%=component.getLabel("refresh."+isRefresh, request.getLocale())%></a></div>
<table width="90%" border="0" class="table" align="center">
  <tr> 
    <td class="tableHead" align="center" width="150"><%=component.getLabel("segment", request.getLocale())%></td>
    <td class="tableHead" align="center" width="150"><%=component.getLabel("size", request.getLocale())%></td>
    <td class="tableHead" align="center" width="150"><%=component.getLabel("state", request.getLocale())%></td>
    <td class="tableHead" align="center"><%=component.getLabel("action", request.getLocale())%></td>
    <td class="tableHead" align="center"><%=component.getLabel("delete", request.getLocale())%></td>
  </tr>
  <!-- list segment in table begin -->
  <%
  
      Path segments = new Path(crawlDir,"segments");
      Path crawldb = new Path(crawlDir,"crawldb");
      Path linkdb = new Path(crawlDir,"linkdb");
      List runningFilesCrawldb = FileUtil.getRunningFiles(crawldb, configuration);
      List runningFilesLinkdb = FileUtil.getRunningFiles(linkdb, configuration);
      boolean isCrawldbRunning = FileUtil.getRunningFiles(crawldb, configuration).size() > 0;
      boolean isLinkdbRunning = FileUtil.getRunningFiles(linkdb, configuration).size() > 0;
      boolean isInjected = FileUtil.isInjected(crawlDir, configuration);
    
      if(isInjected  && !isCrawldbRunning) {
    	  disableGenerate = "";
      }
                
    Path[] files = FileUtil.listFolders(segments, configuration);
    for(int i = 0; i < files.length; i++) {
        String disableCrawldb = "disabled=\"disabled\"";
        String disableIndex = "disabled=\"disabled\"";
        String disableLinkdb = "disabled=\"disabled\"";
        String disableParse = "disabled=\"disabled\"";
        String disableDeleteIndex = "disabled=\"disabled\"";
        String disableDeleteParse = "disabled=\"disabled\"";
        String disablePutToSearch = "disabled=\"disabled\"";
        String disableRemoveFromSearch = "disabled=\"disabled\"";
        String disableDeleteSegment = "disabled=\"disabled\"";
        String disableFetch= "disabled=\"disabled\"";

//        String absoluteSegmentName = files[i].getAbsolutePath();
        String absoluteSegmentName = files[i].toString();
        boolean isFetched = FileUtil.isFetched(files[i], configuration);
        boolean isParsed = FileUtil.isParsed(files[i], configuration);
        boolean isInverted = FileUtil.isInverted(files[i], configuration);
        boolean isIndexed = FileUtil.isIndexed(files[i], configuration);
        boolean isReadyToSearch = FileUtil.isReadyToSearch(files[i], configuration);
        List runningFilesFromSegments = FileUtil.getRunningFiles(files[i], configuration);
        if(runningFilesFromSegments.size() == 0) {
            disableFetch = "";
            disableDeleteSegment="";
            if(isFetched){
                disableFetch= "disabled=\"disabled\"";
            }
            if(isFetched && isParsed && isInverted && !isCrawldbRunning && !isLinkdbRunning && !isIndexed){
             disableIndex = "";        
              }
            if(isFetched && isParsed && !isCrawldbRunning) {
                disableCrawldb = "";
            }
            if(isFetched && isParsed && !isLinkdbRunning) {
                disableLinkdb = "";
            }
            if(isFetched && !isParsed) {
                disableParse = "";
            }
            if(isParsed) {
                disableDeleteParse = "";
            }
            if(isIndexed) {
                disableDeleteIndex = "";
                disablePutToSearch = "";
            }
            if(isReadyToSearch) {
                disableRemoveFromSearch = "";
                disablePutToSearch = "disabled=\"disabled\"";
            }
        }
        
        %>
        
        <tr> 
    <td class="tableCellWhite" align="center"><%=files[i].getName()%></td>
    <td class="tableCellWhite" align="center"><%=FileUtil.size(files[i], configuration)%> bytes</td>
    <td class="tableCellWhite" align="center">
    <%
    Set runningFiles = new HashSet();
    runningFiles.addAll(runningFilesCrawldb);
    runningFiles.addAll(runningFilesLinkdb);
    runningFiles.addAll(runningFilesFromSegments);
    Iterator iter = runningFiles.iterator();
    while (iter.hasNext()) {
      String element = (String) iter.next();
    %>
    <div><%=element%></div>
    <%}%>
    </td>
    <td class="tableCellWhite" align="left"> 
      <table>
        <tr> 
          <td>
              <form method="post" action="index.jsp?refresh=<%=refresh%>">
                 <input type="hidden" name ="action" value="fetch"/>
                 <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
                    <input type="submit" value="<%=component.getLabel("button.fetch", request.getLocale())%>" <%=disableFetch%>/>
            </form>
          </td>
          <td>
              <form method="post" action="index.jsp?refresh=<%=refresh%>">
                 <input type="hidden" name ="action" value="parse"/>
                 <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
                    <input type="submit" value="<%=component.getLabel("button.parse", request.getLocale())%>" <%=disableParse%>/>
            </form>
          </td>
          
          <td> 
            <form method="post" action="index.jsp?refresh=<%=refresh%>">
              <input type="hidden" name ="action" value="updatecrawldb"/>
              <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
              <input type="submit" value="<%=component.getLabel("button.crawldb", request.getLocale())%>"   <%=disableCrawldb%>/>
            </form>
          </td>
          <td> 
            <form method="post" action="index.jsp?refresh=<%=refresh%>">
              <input type="hidden" name ="action" value="invert"/>  
              <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
              <input type="submit" value="<%=component.getLabel("button.linkdb", request.getLocale())%>"  <%=disableLinkdb%>/>
             </form>
          </td>
          
          <td> 
            <form method="post" action="index.jsp?refresh=<%=refresh%>">
              <input type="hidden" name ="action" value="index"/>
                  <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
              <input type="submit" value="<%=component.getLabel("button.index", request.getLocale())%>"  <%=disableIndex%>"/>
             </form>
          </td>
          <!--
          <td>
            <form method="post" action="index.jsp?refresh=<%=refresh%>">
                  <input type="hidden" name ="action" value="putsearch"/>
                  <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
               <input type="submit" value="<%=component.getLabel("button.search.put", request.getLocale())%>"  <%=disablePutToSearch%>/>
        </form>
          </td>
          <td>
           <form method="post" action="index.jsp?refresh=<%=refresh%>">
                  <input type="hidden" name ="action" value="removesearch"/>
                  <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
              <input type="submit" value="<%=component.getLabel("button.search.remove", request.getLocale())%>"  <%=disableRemoveFromSearch%>/>
            </form>
          </td>
         -->
        </tr>
      </table>
    </td>
    <td class="tableCellWhite" align="center">
        <table>
            <tr>
                <td>
            <form method="post" action="index.jsp?refresh=<%=refresh%>">
                  <input type="hidden" name="action" value="deletesegment"/>
                  <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
                   <input type="submit" value="<%=component.getLabel("button.delete.segment", request.getLocale())%>" style="background-color:#DE9393" <%=disableDeleteSegment%>/>
            </form>
                </td>
                <td>
            <form method="post" action="index.jsp?refresh=<%=refresh%>">
                <input type="hidden" name="action" value="deleteindex"/>
                <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
                 <input type="submit" value="<%=component.getLabel("button.delete.index", request.getLocale())%>" style="background-color:#DE9393" <%=disableDeleteIndex%>/>
            </form>                
                </td>
                <td>
            <form method="post" action="index.jsp?refresh=<%=refresh%>">
                <input type="hidden" name="action" value="deleteparse"/>
                <input type="hidden" name ="segment" value="<%=absoluteSegmentName%>"/>
                 <input type="submit" value="<%=component.getLabel("button.delete.parse", request.getLocale())%>" style="background-color:#DE9393" <%=disableDeleteParse%>/>
            </form>                
                </td>
            </tr>
        </table>    
    </td>
  </tr>
  <tr> 
            
        <%
    }
    %>
    <!-- list segment in table end -->    
      
  <tr>
      <td colspan="5" class="tableCellWhite" align="center">
    
      <form method="post" action="index.jsp?refresh=<%=refresh%>">        
       
        <br>
        <input type="hidden" name="action" value="generate"/>
        <input type="submit" value="<%=component.getLabel("button.generate", request.getLocale())%>" style="background-color:#93DEA3" <%=disableGenerate%>/>
        topN: <input name="topn"  size="10" maxlength="50" type="text" value="1000000">
        numFetcher: <input name="fetcher"  size="10" maxlength="50" type="text" value="-1">
        addDays: <input name="days"  size="10" maxlength="50" type="text" value="0">
        </form>
    </td>
</tr>

 
</table>
</body>
</html>

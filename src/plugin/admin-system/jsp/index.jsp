<%@ page
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
%>


<%@ page import="org.apache.hadoop.fs.FileSystem" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.nutch.util.NutchConfiguration" %>
<%@ page import="org.apache.nutch.admin.system.SystemUtil" %>
<%@ page import="java.util.Date" %>
<%@ page import="org.apache.hadoop.mapred.*" %>

<%@ page import="java.util.Collection" %>
<%@ page import="java.util.Iterator" %>
<%@ page import="java.io.File" %>
<%@ page import="java.util.Vector" %>
<%@ page import="org.apache.nutch.admin.*" %>
<%@ page import="java.text.DateFormat" %>
<%@ page import="java.util.Locale" %>
<%@ page import="org.apache.hadoop.hdfs.protocol.DatanodeInfo" %>

<%@ page import="org.apache.hadoop.fs.*" %>


<%
   // 
   // page import="org.apache.hadoop.dfs.DFSShell" 
   // page import="org.apache.hadoop.dfs.DatanodeInfo" 
   // page import="org.apache.hadoop.mapred.TaskTrackerStatus" 
   // page import="org.apache.hadoop.mapred.LocalJobRunner" 

DateFormat dateInstance = DateFormat.getDateTimeInstance(DateFormat.LONG,DateFormat.LONG, request.getLocale());
GuiComponent component = (GuiComponent) application.getAttribute("component");
Configuration configuration = component.getNutchInstance().getConfiguration();
FileSystem fileSystem = FileSystem.get(configuration);
NutchInstance instance = component.getNutchInstance();
Path instanceFolder = instance.getInstanceFolder();

String startTime = "-";
String trackerLabel ="-";
Collection taskTrackers = null;
DatanodeInfo nodeInfos[] = new DatanodeInfo[0];
long totalUsed = 0;
long totalCapacity = 0;
SystemUtil fileSystemUtil = new SystemUtil(configuration, instanceFolder);
nodeInfos = fileSystemUtil.getDataNodesInfos();

//JobClient jobClient = new JobClient();

if(!"local".equals(fileSystem.getName())) {
  
	//JobTracker jobTracker = JobTracker.getTracker();
	//startTime = dateInstance.format(new Date(jobTracker.getStartTime()));
	//trackerLabel = jobTracker.getJobTrackerMachine() + ":" + jobTracker.getTrackerPort();
    //taskTrackers = jobTracker.taskTrackers();
}else {
/*	LocalJobRunner runner = LocalJobRunner.getInstance(configuration);
	trackerLabel = runner.getJobTrackerMachine();
	taskTrackers = new Vector();
	TaskTrackerStatus status = new TaskTrackerStatus("local_tracker", "local", 0, runner.runningJobs(), 0);
	status.setLastSeen(System.currentTimeMillis());
	taskTrackers.add(status);
	startTime = dateInstance.format(new Date(runner.getStartTime()));
  */
}

//ClusterStatus cs = jobClient.getClusterStatus();
// cs.getTaskTrackers();
//startTime = dateInstance.format(cs.getJobTrackerState())


%>



<%@page import="org.apache.hadoop.hdfs.protocol.DatanodeInfo"%>
<%@page import="org.apache.hadoop.mapred.ClusterStatus"%><html>
<head>
<title>Nutch Administration</title>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
<link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<div align="center"> <font class="headline"><%=component.getLabel("system.headline0", request.getLocale())%></font> <a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(<%=component.getLabel("system.help", request.getLocale())%>)</font></a><br>
</div>
<br/>
<div> <font class="headline"><%=component.getLabel("system.headline1", request.getLocale())%></font><br>
  <table width="90%" class="table" align="center">
    <tr>
      <td class="tableHead" align="center" width="50%"><%=component.getLabel("system.ip", request.getLocale())%></td>
      <td class="tableHead" align="center" width="50%"><%=component.getLabel("system.up", request.getLocale())%></td>
    </tr>
    <tr>
      <td class="tableCellWhite" align="center"><%=trackerLabel%></td>
      <td class="tableCellWhite" align="center"><%=startTime%></td>
    </tr>
  </table>
</div>
<br/>
<font class="headline"><%=component.getLabel("system.headline2", request.getLocale())%></font><br>
<table width="90%" class="table" align="center">
  <tr> 
    <td class="tableHead" align="center" width="25%"><%=component.getLabel("system.name", request.getLocale())%></td>
    <td class="tableHead" align="center" width="25%"><%=component.getLabel("system.host", request.getLocale())%></td>
	<td class="tableHead" align="center" width="25%"><%=component.getLabel("system.jobs.running", request.getLocale())%></td>
	<td class="tableHead" align="center" width="25%"><%=component.getLabel("system.heartbeat", request.getLocale())%></td>
  </tr>
  <%--
  <%
  for (Iterator it = taskTrackers.iterator(); it.hasNext(); ) {
	TaskTrackerStatus status = (TaskTrackerStatus) it.next();
	long sinceHeartbeat = System.currentTimeMillis() - status.getLastSeen();
     if (sinceHeartbeat > 0) {
        sinceHeartbeat = sinceHeartbeat / 1000;
      }
      int numCurTasks = 0;
      for (Iterator it2 = status.taskReports(); it2.hasNext(); ) {
          it2.next();
          numCurTasks++;
       }
%>
  <tr> 
    <td class="tableCellWhite" align="center"><%=status.getTrackerName()%></td>
    <td class="tableCellWhite" align="center"><%=status.getHost()%></td>
    <td class="tableCellWhite" align="center"><%=numCurTasks%></td>
	<td class="tableCellWhite" align="center"><%=sinceHeartbeat%></td>
  </tr>
  <%
  }
  %>
  
  
</table>
<font class="headline"><br>
<%=component.getLabel("system.headline0", request.getLocale())%></font> 
<table width="90%" class="table" align="center">
  <tr> 
    <td class="tableHead" align="center" width="20%"><%=component.getLabel("system.name", request.getLocale())%></td>
    <td class="tableHead" align="center" width="20%"><%=component.getLabel("system.space.total", request.getLocale())%></td>
    <td class="tableHead" align="center" width="20%"><%=component.getLabel("system.space.used", request.getLocale())%></td>
    <td class="tableHead" align="center" width="20%">% <%=component.getLabel("system.space.used", request.getLocale())%></td>
	<td class="tableHead" align="center" width="20%"><%=component.getLabel("system.contact", request.getLocale())%></td>
  </tr>

  <% 
  for(int i=0; i<nodeInfos.length; i++){
  	String name = nodeInfos[i].toString();
  	long capacity = nodeInfos[i].getCapacity();
  	long remaining = nodeInfos[i].getRemaining();
  	long different = capacity - remaining; 
  	String capacityString= SystemUtil.byteDesc(capacity);
  	String used = SystemUtil.byteDesc(different);
  	String percent = ""+100*different/capacity;
  %>
  <tr>
  <td class="tableCellWhite" align="center"><%=name%></td>
  <td class="tableCellWhite" align="center"><%=capacityString%></td>
  <td class="tableCellWhite" align="center"><%=used%></td>
  <td class="tableCellWhite" align="center">
    <table width="99" class="progressbar" cellpadding="0" cellspacing="0">
      <tr> 
        <td><img src="gfx/progressbar_red.gif" width="<%=percent%>" height="15"></td>
      </tr>
    </table>
      <%=percent%> %</td>
	<td class="tableCellWhite" align="center"><%=dateInstance.format(new Date(nodeInfos[i].lastUpdate()))%></td>
  </tr>  
  <%
  }
  %>
  --%>
 
</table>
</body>
</html>

<%@ page
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
%>


<%@ page import="org.apache.hadoop.mapred.JobStatus" %>
<%@ page import="org.apache.hadoop.mapred.JobTracker" %>
<%@ page import="org.apache.hadoop.mapred.JobInProgress" %>
<%@ page import="org.apache.hadoop.mapred.JobProfile" %>
<%@ page import="org.apache.hadoop.mapred.TaskReport" %>
<%@ page import="java.util.Vector" %>
<%@ page import="java.util.Iterator" %>
<%@ page import="java.util.Date" %>
<%@ page import="java.lang.StringBuffer" %>
<%@ page import="org.apache.nutch.admin.*" %>
<%@ page import="org.apache.hadoop.mapred.LocalJobRunner" %>
<%@ page import="org.apache.hadoop.mapred.LocalJobRunner.Job" %>
<%@ page import="org.apache.hadoop.fs.FileSystem" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.nutch.util.NutchConfiguration" %>
<%@ page import="java.text.DateFormat" %>
<%@ page import="java.util.Locale" %>


<%!

public String generateTable(String label, TaskReport[] tasks, GuiComponent component, Locale locale) {
StringBuffer buffer = new StringBuffer();
buffer.append("<div> <font class=\"headline\"><br>" + label + "</font></div>");
  
  
buffer.append("<div>");
buffer.append("<table width=\"90%\" class=\"table\" align=\"center\">");
buffer.append("<tr>"); 
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"20%\">"+component.getLabel("map.id", locale)+"</td>");
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"20%\">% "+component.getLabel("complete", locale)+"</td>");
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"20%\">"+component.getLabel("state", locale)+"</td>");
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"20%\">"+component.getLabel("diagnostic", locale)+"</td>");
buffer.append("</tr>");

for (int i = 0; i < tasks.length; i++) {
      TaskReport report = tasks[i];
      String tipid = report.getTaskId();
      String progress = String.valueOf(report.getProgress()*100);
      String state = report.getState();
      String[] diagnostics = report.getDiagnostics();
      int diagnosticSize = diagnostics.length;
      

buffer.append("<tr>");
buffer.append("<td class=\"tableCellWhite\" align=\"center\">"+tipid+"</td>");
buffer.append("<td class=\"tableCellWhite\" align=\"center\">");
buffer.append("<table width=\"100\" class=\"progressbar\" cellpadding=\"0\" cellspacing=\"0\">");
buffer.append("<tr>");
buffer.append("<td><img src=\"gfx/progressbar.gif\" width=\""+progress+"\" height=\"15\"></td>");
buffer.append("</tr>");
buffer.append("</table>");
buffer.append(""+progress+" %</td>");

buffer.append("<td class=\"tableCellWhite\" align=\"center\">"+state+"</td>");
      
      for (int j = 0; j < diagnosticSize; j++) {
		buffer.append("<td class=\"tableCellWhite\" align=\"center\">"+diagnostics[j]);
		buffer.append("<b>");
      }
}

buffer.append("</table>");
buffer.append("</div>");
return buffer.toString();
}

%>


<%
DateFormat dateInstance = DateFormat.getDateTimeInstance(DateFormat.LONG,DateFormat.LONG, request.getLocale());
String refresh = request.getParameter("refresh") != null ? request.getParameter("refresh") : "off";
String isRefresh = "off".equals(refresh) ? "on" : "off";

String jobid = request.getParameter("jobid") != null ? request.getParameter("jobid") : "";
  
boolean isLocal=false;
GuiComponent component = (GuiComponent) application.getAttribute("component");
Configuration configuration = component.getNutchInstance().getConfiguration();
FileSystem fileSystem = FileSystem.get(configuration);
String message ="";
TaskReport[] reduceTaskReports = new TaskReport[0];
TaskReport[] mapTaskReports = new TaskReport[0];
int desiredMaps = 0;
int desiredReduces = 0;
int completedMaps = 0;
int completedReduces = 0;
JobStatus status = null;
String startTime = "-";
String finishTime = "-";
String statePicture = "";
JobProfile profile = null;

if(!"local".equals(fileSystem.getName())) {
	JobTracker tracker = JobTracker.getTracker();
	JobInProgress job = (JobInProgress) tracker.getJob(jobid);
	mapTaskReports = (job != null) ? tracker.getMapTaskReports(jobid) : null;
	reduceTaskReports = (job != null) ? tracker.getReduceTaskReports(jobid) : null;
	desiredMaps = (job != null) ? job.desiredMaps() : 0;
	desiredReduces = (job != null) ? job.desiredReduces() : 0;
	completedMaps = (job != null) ? job.finishedMaps() : 0;
	completedReduces = (job != null) ? job.finishedReduces() : 0;
	profile = (job != null) ? (job.getProfile()) : null;
	startTime = dateInstance.format(new Date(job.getStartTime()));
	status = (job != null) ? (job.getStatus()) : null;
	if (status.getRunState() == JobStatus.RUNNING) {
	  	statePicture = "gfx/trafficlight_green.gif";
	  } else if (status.getRunState() == JobStatus.SUCCEEDED) {
	  	finishTime = dateInstance.format(new Date(job.getFinishTime()));
	  	statePicture = "gfx/trafficlight_green.gif";
	  } else if (status.getRunState() == JobStatus.FAILED) {
	    finishTime = dateInstance.format(new Date(job.getFinishTime()));
	    statePicture = "gfx/trafficlight_red.gif";
	  }
}else if("local".equals(fileSystem.getName())){
	isLocal=true;
	LocalJobRunner runner = LocalJobRunner.getInstance(configuration);
	Job job = (Job) runner.getJob(jobid);
	desiredMaps = (job != null) ? job.getDesiredMaps() : 0;
	desiredReduces = (job != null) ? job.getDesiredReduces() : 0;
	completedMaps = (job != null) ? job.getFinishedMaps() : 0;
	completedReduces = (job != null) ? job.getFinishedReduces() : 0;
	profile = runner.getJobProfile(jobid);
	mapTaskReports = (job != null) ? runner.getMapTaskReports(jobid) : null;
	reduceTaskReports = (job != null) ? runner.getReduceTaskReports(jobid) : null;
	startTime = dateInstance.format(new Date(job.getStartTime()));
	status = (job != null) ? (job.getStatus()) : null;
	if (status.getRunState() == JobStatus.RUNNING) {
	  	statePicture = "gfx/trafficlight_green.gif";
	  } else if (status.getRunState() == JobStatus.SUCCEEDED) {
	  	finishTime = dateInstance.format(new Date(job.getFinishTime()));
	  	statePicture = "gfx/trafficlight_green.gif";
	  } else if (status.getRunState() == JobStatus.FAILED) {
	    finishTime = dateInstance.format(new Date(job.getFinishTime()));
	    statePicture = "gfx/trafficlight_red.gif";
	  }
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
<div align="center"> <font class="headline"><%=component.getLabel("job.detail.headline", request.getLocale())%></font> <a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(<%=component.getLabel("job.help", request.getLocale())%>)</font></a><br>
    <br>
</div>
<div align="center"><a href="?jobid=<%=jobid%>&refresh=<%=isRefresh%>"><%=component.getLabel("refresh."+isRefresh, request.getLocale())%></a></div>
<div> <font class="headline"><%=component.getLabel("general", request.getLocale())%></font> <br>
</div>

<table width="90%" class="table" align="center">
  <tr>
    <td class="tableHead" align="center" width="8%"><%=component.getLabel("job.id", request.getLocale())%></td>
    <td class="tableHead" align="center" width="8%"><%=component.getLabel("state", request.getLocale())%></td>
	<td class="tableHead" align="center" width="8%"><%=component.getLabel("type", request.getLocale())%></td>
    <td class="tableHead" align="center" width="8%"><%=component.getLabel("start", request.getLocale())%></td>
	<td class="tableHead" align="center" width="8%"><%=component.getLabel("finish", request.getLocale())%></td>
    <td class="tableHead" align="center" width="8%"><%=component.getLabel("job.file", request.getLocale())%></td>
	<td class="tableHead" align="center" width="8%"><%=component.getLabel("map.required", request.getLocale())%></td>
	<td class="tableHead" align="center" width="8%"><%=component.getLabel("map.complete", request.getLocale())%></td>
	<td class="tableHead" align="center" width="8%"><%=component.getLabel("reduce.required", request.getLocale())%></td>
	<td class="tableHead" align="center" width="8%"><%=component.getLabel("reduce.complete", request.getLocale())%></td>	    
  </tr>
  <tr>
    <td class="tableCellWhite" align="center"><%=jobid%></td>
    <td class="tableCellWhite" align="center"><img src=<%=statePicture%> width="33" height="16"></td>
	<td class="tableCellWhite" align="center"><%=profile.getJobName()%></td>
    <td class="tableCellWhite" align="center"><%=startTime%></td>
    
    

	<td class="tableCellWhite" align="center"><%=finishTime%></td>
    <td class="tableCellWhite" align="center"><%=profile.getJobFile()%></td>
	<td class="tableCellWhite" align="center"><%=desiredMaps%></td>
	<td class="tableCellWhite" align="center"><%=completedMaps%></td>
	<td class="tableCellWhite" align="center"><%=desiredReduces%></td>
	<td class="tableCellWhite" align="center"><%=completedReduces%></td>
   </tr>  
</table>


<%=generateTable(component.getLabel("map.headline", request.getLocale()), mapTaskReports, component, request.getLocale())%>
<%=generateTable(component.getLabel("reduce.headline", request.getLocale()), reduceTaskReports, component, request.getLocale())%>


</body>
</html>

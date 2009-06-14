<%@ page
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
%>


<%@ page import="org.apache.hadoop.fs.FileSystem" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.nutch.util.NutchConfiguration" %>
<%@ page import="org.apache.hadoop.dfs.DFSShell" %>
<%@ page import="org.apache.hadoop.dfs.DatanodeInfo" %>
<%@ page import="java.util.Date" %>
<%@ page import="org.apache.hadoop.mapred.JobTracker" %>
<%@ page import="org.apache.hadoop.mapred.LocalJobRunner" %>
<%@ page import="org.apache.hadoop.mapred.LocalJobRunner.Job" %>
<%@ page import="org.apache.hadoop.mapred.JobInProgress" %>
<%@ page import="org.apache.hadoop.mapred.JobStatus" %>
<%@ page import="org.apache.hadoop.mapred.JobProfile" %>
<%@ page import="java.util.Collection" %>
<%@ page import="java.util.Iterator" %>
<%@ page import="java.util.Vector" %>
<%@ page import="java.io.IOException" %>
<%@ page import="org.apache.hadoop.mapred.TaskTrackerStatus" %>
<%@ page import="java.lang.StringBuffer" %>
<%@ page import="org.apache.nutch.admin.*" %>
<%@ page import="java.text.DateFormat" %>
<%@ page import="java.util.Locale" %>





<%!


 public String generateTable(String label, Vector jobs, boolean local, GuiComponent component, Locale locale) throws IOException {
 StringBuffer buffer = new StringBuffer();
 buffer.append("<div><font class=\"headline\">"+label+"</font><br></div>");
buffer.append("<table width=\"90%\" class=\"table\" align=\"center\">");
buffer.append("<tr>"); 
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"8%\">"+component.getLabel("job.id", locale)+"</td>");
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"8%\">"+component.getLabel("start", locale)+"</td>");
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"8%\">"+component.getLabel("finish", locale)+"</td>");
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"8%\">% "+component.getLabel("complete", locale)+"</td>");
buffer.append("<td class=\"tableHead\" align=\"center\" width=\"12%\">&nbsp;</td>");
buffer.append("</tr>");
DateFormat dateInstance = DateFormat.getDateTimeInstance(DateFormat.LONG,DateFormat.LONG, locale);
    
for (Iterator it = jobs.iterator(); it.hasNext(); ) {
    JobStatus status = null;
    String startTime = "-";
    String finishTime="-";
    String jobid = "";
    if(!local) {
	    JobInProgress job = (JobInProgress) it.next();
    		JobProfile profile = job.getProfile();
    		status = job.getStatus();
    		jobid = profile.getJobId();
    		startTime = dateInstance.format(new Date(job.getStartTime()));
    		if(job.getFinishTime()!=0){
    	  		finishTime = dateInstance.format(new Date(job.getFinishTime()));
    	  	}
    }else {
    		Job job = (Job) it.next();
    		jobid = job.getJobId();
    		status = job.getStatus();
    		startTime = dateInstance.format(new Date(job.getStartTime()));
    		if(job.getFinishTime()!=0){
    	  		finishTime = dateInstance.format(new Date(job.getFinishTime()));
    	  	}
    }
    
    
    double completedRatio = (0.5 * (100 * status.mapProgress())) +
                                 (0.5 * (100 * status.reduceProgress()));


buffer.append("<tr>"); 
buffer.append("<td class=\"tableCellWhite\" align=\"center\"><a href=\"jobDetail.jsp?jobid="+jobid+"\">"+jobid+"</a></td>");
buffer.append("<td class=\"tableCellWhite\" align=\"center\">"+startTime+"</td>");
buffer.append("<td class=\"tableCellWhite\" align=\"center\">"+finishTime+"</td>");
buffer.append("<td class=\"tableCellWhite\" align=\"center\">");
buffer.append("<table width=\"100\" class=\"progressbar\" cellpadding=\"0\" cellspacing=\"0\">");
buffer.append("<tr>");
buffer.append("<td><img src=\"gfx/progressbar.gif\" width=\""+completedRatio+"\" height=\"15\"></td>");
buffer.append("</tr>");
buffer.append("</table>");
buffer.append(""+completedRatio+"%</td><td class=\"tableCellWhite\" align=\"center\">");
buffer.append("<form method=\"post\" action=\"index.jsp\">");
buffer.append("<input type=\"hidden\" name=\"action\" value=\"abort\" ");
buffer.append("<input type=\"hidden\" name=\"jobid\" value=\""+jobid+"\"");
buffer.append("<input type=\"submit\" value=\""+component.getLabel("job.abort", locale)+"\" style=\"background-color:#DE9393\">");
buffer.append("</form>");

 

}
buffer.append("</td>");
buffer.append("</tr>");
buffer.append("</table>");
 return buffer.toString();
 }
%>

<%
String refresh = request.getParameter("refresh") != null ? request.getParameter("refresh") : "off";
String isRefresh = "off".equals(refresh) ? "on" : "off";
boolean isLocal=false;
Vector completedJobs = new Vector();
Vector failedJobs = new Vector();
Vector runningJobs = new Vector();
GuiComponent component = (GuiComponent) application.getAttribute("component");
Configuration configuration = component.getNutchInstance().getConfiguration();
FileSystem fileSystem = FileSystem.get(configuration);
String message ="";
String action = request.getParameter("action");
String jobid = request.getParameter("jobid") != null ? request.getParameter("jobid") : "";

if(!"local".equals(fileSystem.getName())) {
	JobTracker jobtracker = JobTracker.getTracker();
	
	if("abort".equals(action)) {
		if(jobtracker.getJob(jobid) != null) {
			jobtracker.killJob(jobid);
		}
	}
	
	runningJobs = jobtracker.runningJobs();
	failedJobs = jobtracker.failedJobs();
	completedJobs = jobtracker.completedJobs();
}else {
	LocalJobRunner runner = LocalJobRunner.getInstance(configuration);
	
	if("abort".equals(action)) {
		if(runner.getJob(jobid) != null) {
			runner.killJob(jobid);
		}
	}
	
	runningJobs = runner.runningJobs();
	failedJobs = runner.failedJobs();
	completedJobs = runner.completedJobs();
	isLocal=true;	
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
<div align="center"> <font class="headline"><%=component.getLabel("job.headline", request.getLocale())%></font> <a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(<%=component.getLabel("job.help", request.getLocale())%>)</font></a><br>
    <br>
</div>
<div align="center"><a href="?refresh=<%=isRefresh%>"><%=component.getLabel("refresh."+isRefresh, request.getLocale())%></a></div>


<%=generateTable(component.getLabel("job.running", request.getLocale()), runningJobs, isLocal, component, request.getLocale())%>
<%=generateTable(component.getLabel("job.complete", request.getLocale()), completedJobs, isLocal, component, request.getLocale())%>
<%=generateTable(component.getLabel("job.fail", request.getLocale()), failedJobs, isLocal, component, request.getLocale())%>

</html>

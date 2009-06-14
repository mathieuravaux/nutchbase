<%@ page import="org.apache.nutch.admin.*" %>
<%@ page import="org.apache.nutch.admin.scheduling.*" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.hadoop.fs.*" %>
<%@ page import="java.util.Map" %>
<%@ page import="java.util.HashMap" %>
<%@ page import="org.quartz.Scheduler" %>


<%
String time="";
String day="";
String cronPattern="";
String message ="empty_label";
String action = request.getParameter("action");
GuiComponent component = (GuiComponent) application.getAttribute("component");
Configuration configuration = component.getNutchInstance().getConfiguration();
PathSerializable instanceFolder = new PathSerializable(component.getNutchInstance().getInstanceFolder());
SchedulingService schedulerService = (SchedulingService) application.getAttribute("scheduler");
if(schedulerService == null) {
	schedulerService = new SchedulingService(instanceFolder);
	application.setAttribute("scheduler", schedulerService);
}
try {
if(component.getLabel("scheduling.button.delete", request.getLocale()).equals(action)) {
	schedulerService.deleteJob(SchedulingService.CRAWL_JOB, Scheduler.DEFAULT_GROUP);
}
}catch(org.quartz.SchedulerException e) {
	message=e.getMessage();
}

if (component.getLabel("scheduling.button.create", request.getLocale()).equals(action) && request.getParameter("time") != null && request.getParameter("day") != null) { 
	Map<String, PathSerializable> data = new HashMap<String, PathSerializable>();
	data.put("crawldb", new PathSerializable(instanceFolder, "crawldb"));
	data.put("linkdb", new PathSerializable(instanceFolder, "linkdb"));
	data.put("segments", new PathSerializable(instanceFolder, "segments"));
	data.put("configuration", instanceFolder);
	time = request.getParameter("time");
    day = request.getParameter("day");
   	cronPattern = time + " " + day;
   	schedulerService.scheduleCronJob(SchedulingService.CRAWL_JOB, Scheduler.DEFAULT_GROUP, AdminCrawl.class, data, cronPattern);
}

String expression = "";
String disableButton = "";
try {
String tmpString  = schedulerService.getCronExpressions(SchedulingService.CRAWL_JOB, Scheduler.DEFAULT_GROUP);
disableButton =  tmpString!=null ? "" : "\"disabled=true\"";
expression = tmpString!=null ? tmpString : "";
}catch(org.quartz.SchedulerException e) {
	message=e.getMessage();
}

%>

<html>
<head>
<title>Nutch Administration</title>
<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
<link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<div align="center"> 
	<font class="headline"><%=component.getLabel("scheduling.scheduling", request.getLocale())%></font> 
		<a href="help.jsp"><font face="Arial, Helvetica, sans-serif" size="2">(<%=component.getLabel("scheduling.help", request.getLocale())%>)</font></a>
		<br>
  <font face="Arial, Helvetica, sans-serif" size="2"><%=component.getLabel("scheduling.summary", request.getLocale())%></font> <br>
</div>
<div> <font class="headline"><br>
  <%=component.getLabel("scheduling.headline0", request.getLocale())%></font> </div>
<form method="post" action="index.jsp">
  <table width="90%" class="table" align="center">
    <tr> 
      <td colspan="2" class="tableHead"><%=component.getLabel("scheduling.headline1", request.getLocale())%></td>
    </tr>
    <tr> 
      <td width="150" class="tableCellGrey"><%=component.getLabel("scheduling.pattern", request.getLocale())%></td>
      <td class="tableCellWhite">
        <%=expression%>
		<input type="submit" name="action" value="<%=component.getLabel("scheduling.button.delete", request.getLocale())%>"  <%=disableButton%> style="background-color:#DE9393">
      </td>	
    </tr>
  </table>
</form>


<span class="headline"><%=component.getLabel("scheduling.headline2", request.getLocale())%></span>
       <form method="post" action="index.jsp">
       <table class="table" width="400" align="center">
           <tr>
               <td colspan="2" class="tablehead"><%=component.getLabel("scheduling.headline3", request.getLocale())%></td>
           </tr>
           <tr>
               <td class="tablecell" width="100"><%=component.getLabel("scheduling.hour", request.getLocale())%>:<%=component.getLabel("scheduling.minute", request.getLocale())%></td>
               <td class="tablecell">
                   <select name="time" style="width:100%">  
                      <optgroup label="<%=component.getLabel("scheduling.fixed.time", request.getLocale())%>">
                           <%
                           for(int i = 0; i < 24; i++){
                           %>
                               <option value="0 0 <%=i%>"<%if(time.equals("0 0 "+i)) {%> selected="selected"<%}%>><%if(i < 10){%>0<%}%><%=i%>:00</option>
                           <%} %>
                       </optgroup>
                       <optgroup label="<%=component.getLabel("scheduling.auto.repeat", request.getLocale())%>">
                           <option value="0 0 0/1" <%if(time.equals("0 0 0/1")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.hour", request.getLocale())%></option>
                           <option value="0 0 0/2" <%if(time.equals("0 0 0/2")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.2.hour", request.getLocale())%></option>
                           <option value="0 0 0/4" <%if(time.equals("0 0 0/4")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.4.hour", request.getLocale())%></option>
                           <option value="0 0 0/6" <%if(time.equals("0 0 0/6")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.6.hour", request.getLocale())%></option>
                           <option value="0 0 0/8" <%if(time.equals("0 0 0/8")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.8.hour", request.getLocale())%></option>
                           <option value="0 0 0/12" <%if(time.equals("0 0 0/12")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.12.hour", request.getLocale())%></option>
                       </optgroup>                                                                      
                    </select>
                 </td>
           </tr> 
             <tr>
               <td class="tablecell" width="100"><%=component.getLabel("scheduling.day", request.getLocale())%> / <%=component.getLabel("scheduling.week", request.getLocale())%>:</td>
               <td class="tablecell">
                   <select name="day" style="width:100%">
                       <option value="* * ?"><%=component.getLabel("scheduling.daily", request.getLocale())%></option>
                       <optgroup label="<%=component.getLabel("scheduling.fixed.weekday", request.getLocale())%>">
                           <option value="? * MON"  <%if(day.equals("? * MON")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.only.monday", request.getLocale())%></option>
                           <option value="? * TUE"  <%if(day.equals("? * TUE")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.only.tuesday", request.getLocale())%></option>
                           <option value="? * WED"  <%if(day.equals("? * WED")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.only.wednesday", request.getLocale())%></option>
                           <option value="? * THU"  <%if(day.equals("? * THU")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.only.thursday", request.getLocale())%></option>
                           <option value="? * FRI"  <%if(day.equals("? * FRI")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.only.friday", request.getLocale())%></option>
                           <option value="? * SAT"  <%if(day.equals("? * SAT")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.only.saturday", request.getLocale())%></option>
                           <option value="? * SUN"  <%if(day.equals("? * SUN")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.only.sunday", request.getLocale())%></option>
                       </optgroup>
                       <optgroup label="<%=component.getLabel("scheduling.selected.weekday", request.getLocale())%>">
                           <option value="? * MON-FRI"  <%if(day.equals("? * MON-FRI")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.monday.friday", request.getLocale())%></option>
                           <option value="? * MON,THU"  <%if(day.equals("? * MON,THU")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.monday.thursday", request.getLocale())%></option>
                           <option value="? * TUE,FRI"  <%if(day.equals("? * TUE,FRI")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.tuesday.friday", request.getLocale())%></option>
                           <option value="? * MON,WED,FRI"  <%if(day.equals("? * MON,WED,FRI")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.monday.wednesday.friday", request.getLocale())%></option>
                           <option value="? * TUE,THU,SAT"  <%if(day.equals("? * TUE,THU,SAT")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.tuesday.thursday.saturday", request.getLocale())%></option>
                       </optgroup>
                       <optgroup label="<%=component.getLabel("scheduling.auto.repeat", request.getLocale())%>">
                           <option value="1/2 * ?"  <%if(day.equals("1/2 * ?")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.2.days", request.getLocale())%></option>
                           <option value="1/3 * ?"  <%if(day.equals("1/3 * ?")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.3.days", request.getLocale())%></option>
                           <option value="1/4 * ?"  <%if(day.equals("1/4 * ?")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.4.days", request.getLocale())%></option>
                           <option value="1/5 * ?"  <%if(day.equals("1/5 * ?")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.every.5.days", request.getLocale())%></option>                               
                           <option value="1,14,L * ?"  <%if(day.equals("1,14,L * ?")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.first.14.last.days", request.getLocale())%></option>
                       </optgroup>                                           
                       <optgroup label="<%=component.getLabel("scheduling.fixed.day.month", request.getLocale())%>">                                                      <%
                           for(int i = 1; i < 32; i++){
                           	String loopLabel="scheduling."+i+".day.month";
                           %>
                               <option value="<%=i%> * ?"<%if(day.equals(i+" * ?")) {%> selected="selected"<%}%>><%=component.getLabel(loopLabel, request.getLocale())%></option>
                           <%} %>
                           <option value="L * ?"  <%if(day.equals("L * ?")) {%>selected="selected"<%}%>><%=component.getLabel("scheduling.last.day.month", request.getLocale())%></option>
                       </optgroup>
                   </select>
               </td>
           </tr>                                                           </table>
       <br/>
       <input type="submit" name="action" value="<%=component.getLabel("scheduling.button.create", request.getLocale())%>"/>
       </form>


</body>
</html>

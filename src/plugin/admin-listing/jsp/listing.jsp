<%@ page contentType="text/html; charset=UTF-8"
pageEncoding="UTF-8" 
%>
<%@ page  import="org.apache.nutch.plugin.Extension"%>
<%@ page import="org.apache.nutch.admin.GuiComponent" %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.nutch.admin.NutchInstance" %>
<html>
<head>
<link rel="stylesheet" href="<%=request.getContextPath()%>/css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<table border="0" cellpadding="0" cellspacing="0">
  <tr>
<%

GuiComponent[] components = (GuiComponent[]) application.getAttribute("components");
for(int i=0; i<components.length; i++) {
	Extension extension = components[i].getExtension();
	String link = "";
	if("admin-listing".equals(extension.getDescriptor().getPluginId())) {
		continue;
	}else {
		link = extension.getDescriptor().getPluginId();
	}
	
	String tabname = components[i].getLabel(extension.getAttribute("tabName"), request.getLocale());
	%>



    <td class="tab">
		<img src="<%=request.getContextPath()%>/gfx/tabLeft.gif" border="0"> 
      	<a href="<%=request.getContextPath()+"/"+link+"/"%>index.jsp" target="mainFrame" class="tab"><%=tabname%></a> 
		<img src="<%=request.getContextPath()%>/gfx/tabRight.gif" border="0"> 
    </td>

<%
}
%>
    </tr>
    </table>
</body>
</html>
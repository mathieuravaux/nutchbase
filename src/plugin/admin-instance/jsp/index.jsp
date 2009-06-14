<%@ page
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
%>
<%@ page import="org.apache.nutch.admin.*"%>
<%@ page import="java.io.File"%>
<%@ page import="org.apache.hadoop.fs.*"%>
<%@ page import="org.apache.nutch.plugin.Extension"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>
<%
NutchInstance[] instances = (NutchInstance[]) application.getAttribute("instances");
GuiComponent component = (GuiComponent) application.getAttribute("component");
NutchInstance instance = component.getNutchInstance();
Configuration configuration = instance.getConfiguration();
Path instanceFolder = instance.getInstanceFolder();
String newConfig = request.getParameter("newconfig");
LocalFileSystem fs = new LocalFileSystem(null);
String message = "";
if(newConfig!=null && !"".equals(newConfig)) {
		Path newInstanceFolder = new Path(instanceFolder, newConfig);
		if(!fs.exists(newInstanceFolder)){
			GuiConfigUtil.createConfiguration(newInstanceFolder);
			Extension[] extensions = AdministrationApp.getComponentExtensions(configuration, GuiComponent.IS_INSTANCE_COMPONENT);
			WebContainer webContainer = (WebContainer) application.getAttribute("container");
			NutchInstance newNutchInstance = AdministrationApp.loadNutchInstance(configuration, newInstanceFolder);
			webContainer.addComponentExtensions(extensions, newNutchInstance, instances);
			NutchInstance[] newInstances  = new NutchInstance[instances.length +1];
			System.arraycopy(instances, 0, newInstances, 0, instances.length);
			newInstances[instances.length] = newNutchInstance;
			application.setAttribute("instances", newInstances);
			instances = newInstances;
	} else {
		message = component.getLabel("folderAlreadyExist", request.getLocale());
	}
}

%>

<%@page import="org.apache.hadoop.fs.LocalFileSystem"%>
<html>
<head>
<title>Nutch Adminitration</title>
<link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<div style="color:red"><%=message%></div>
<br />

<div align="left"><font class="headline"><%=component.getLabel("instances.headline0", request.getLocale())%></font>
<br>
</div>

<%
System.out.println("instances: " + instances);
for(int i=0; i<instances.length; i++) {
String name = instances[i].getName();
%>
<a href="<%=request.getContextPath()+"/../../"+name%>"
	target="newwindow"><%=name%></a>
<br />
<%
}
%>
<br />
<br />
<div align="left"><font class="headline"><%=component.getLabel("instances.headline1", request.getLocale())%></font>
<br>
</div>
<form method="post" action="index.jsp">
<table width="90%" border="0" class="table" align="center">
	<tr>
		<td colspan="2" class="tableHead"><%=component.getLabel("instances.headline2", request.getLocale())%></td>
	</tr>
	<tr>
		<td width="150" class="tableCellGrey"><%=component.getLabel("instances.url", request.getLocale())%></td>
		<td><input name="newconfig" size="24" maxlength="128" type="text""></td>
	</tr>
	<tr>
		<td width="150" class="tableCellWhite">&nbsp;</td>
		<td class="tableCellWhite"><input type="submit"
			value="<%=component.getLabel("instances.button.submit", request.getLocale())%>" /></td>
	</tr>
</table>
</form>
</body>
</html>

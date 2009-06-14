<%@ page import="org.apache.nutch.admin.*" 
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"
%>
<%
GuiComponent component = (GuiComponent) application.getAttribute("component");
String headline = component.getLabel("listing.headline",request.getLocale());
String aut = component.getLabel("listing.aut",request.getLocale());
%>
<html>
<head>
<title>Nutch Administration</title>
<link rel="stylesheet" href="css/admingui.css" type="text/css">
</head>

<body bgcolor="#FFFFFF" text="#000000">
<div align="center"> <font class="headline"><%=headline%></font> <br>
  <font face="Arial, Helvetica, sans-serif" size="2"><%=aut%></font>. 
  <br>
  <br>
</div>
</body>
</html>

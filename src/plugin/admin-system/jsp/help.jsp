<%@ page import="org.apache.nutch.admin.*" %>

<%
GuiComponent component = (GuiComponent) application.getAttribute("component");
String help = component.getLabel("help", request.getLocale());
%>

<html>
<head>
</head>
<body>
<div><%=help%></div>
</body>
</html>
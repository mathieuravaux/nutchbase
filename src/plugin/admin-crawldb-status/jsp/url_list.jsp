<%@ page import="org.apache.nutch.crawl.*"%>
<%@ page import="org.apache.nutch.admin.*"%>
<%@ page import="org.apache.nutch.admin.crawldb.*"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>
<%@ page import="org.apache.commons.lang.StringEscapeUtils"%>

<%@ page import="org.apache.hadoop.fs.*"%>
<%@ page import="org.apache.hadoop.io.*"%>
<%@ page import="java.io.*"%>
<%@ page import="java.util.*"%>
<%@ page import="java.net.URL"%>


<%
GuiComponent component = (GuiComponent) application.getAttribute("component");

Map map = (Map)request.getAttribute("map");

String statusdrop = (String)request.getAttribute("statusdrop");
byte statusFilter = Byte.parseByte((String)request.getAttribute("statusFilter"));
String urlFilter = (String)request.getAttribute("urlFilter");
int pageIndex = ((Integer)request.getAttribute("pageIndex")).intValue();

%>

<%@page import="java.text.DecimalFormat"%><html>
    <head>
        <title>Nutch Crawldb Statistics</title>
        <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
        <link rel="stylesheet" href="css/admingui.css" type="text/css">
    </head>
    <body bgcolor="#FFFFFF" text="#000000">
        <form method="post" action="index.jsp">
            <input type='hidden' name="status" value='crawlstatus' /> 
            <input type='hidden' name="reset" value='true' /> 
            <table width="100%" border="0" class="table" align="center">
                <tr>
                    <td colspan="2" class="tableHead"><%=component.getLabel("crawldb.url.filters", request.getLocale())%></td>
                    <td width="160px" class="tableHead"> Suppressions</td> 
                </tr>
                <tr class="tableCellGrey">
                    <td width="20">
                        <%=component.getLabel("crawldb.url.urlfilter", request.getLocale())%>
                    </td>
                    <td width="300">
                        <input name="urlFilter" size="80" maxlength="100" type="text" value="<%=urlFilter%>">
                    </td>
                    <td id="domainsToDeleteCount">&nbsp;</td>
                </tr>
                <tr class="tableCellGrey">
                    <td width="200">
                        <%=component.getLabel("crawldb.url.statusfilter", request.getLocale())%>
                    </td>
                    <td>
                        <select name="statusdrop">
                        <option value="all"><%=component.getLabel("crawldb.url.nostatusfilter", request.getLocale())%></option>
                        <%
                               byte[] statuses = {
                                  CrawlDatum.STATUS_SIGNATURE,
                                  CrawlDatum.STATUS_DB_UNFETCHED,
                                  CrawlDatum.STATUS_DB_FETCHED,
                                  CrawlDatum.STATUS_DB_GONE,
                                  CrawlDatum.STATUS_LINKED,
                                  CrawlDatum.STATUS_FETCH_SUCCESS,
                                  CrawlDatum.STATUS_FETCH_RETRY,
                                  CrawlDatum.STATUS_FETCH_GONE
                                };

                            for(int i=0; i<statuses.length; i++){
                                byte b = statuses[i];
                                out.print("<option value=" + b + " ");
                                if(b == statusFilter){
                                    out.print("selected='selected' "); 
                                }
                                out.println(">" + CrawlDatum.getStatusName(b) + "</option>");
                            }
                        %>
                        </select>
                    </td>
                    <td> &nbsp; </td>
                </tr>
                <tr>
                    <td class="tableCellWhite">
                        <input type="submit" value="<%=component.getLabel("submit.button", request.getLocale())%>"/>
                    </td>
                </tr>
            </table>
        </form>
    
    <table width="100%" id="urlTable" cellpadding=0 cellspacing=0>
    <tr>
        <td class="tableHead">Url</td>
        <td class="tableHead">Score</td>
        <td class="tableHead">Status</td>
        <td class="tableHead">Fetch time</td>
        <td class="tableHead">Metadata</td>
        <td class="tableHead" width="80px">Delete</td>
    </tr>
     
    <%
        Set keys = map.keySet();
        Iterator iter = keys.iterator();
        DecimalFormat scoreFormatter = new DecimalFormat("#####,0000");
        while(iter.hasNext()) {
         String key = (String)iter.next();
         String disp_key = StringEscapeUtils.escapeHtml(key);
         CrawlDatum cd = (CrawlDatum) map.get(key); 
         %>
         <tr>
            <td><a href='<%=disp_key%>' target='_blank'><%=disp_key%></a></td>
            <td><%= scoreFormatter.format(cd.getScore()) %></td> 
            <td> <%= cd.getStatusName(cd.getStatus()) %> </td> <!-- cd.getStatus()   --> 
            <td>&nbsp;</td> <!-- % =new Date(cd.getFetchTime()).toLocaleString() %  -->
            <!--  <td>< % =new Date(cd.getModifiedTime()).toLocaleString() % ></td>-->
            <td>&nbsp;
            <%
             /*for (Map.Entry<Writable, Writable> e : cd.getMetaData().entrySet()) {
                 out.print(e.getKey());
                 out.print(": ");
                 out.print(e.getValue());
                 out.print("<br/>");
            }*/
            %>
            </td>
            <td width="80px">&nbsp;
               <a href="#" class="delete" onclick="deleteUrl(this.parentNode.parentNode); return false">delete</a></td>
            </td>
         </tr>
      <%                    
        }
      %> 
        <tr><td>
      <%
        String prevPageCaption = component.getLabel("crawldb.previous", request.getLocale());
        String nextPageCaption = component.getLabel("crawldb.next", request.getLocale());
      
        if(pageIndex > 0){
            %> <a href="index.jsp?status=crawlstatus&statusdrop=<%=statusdrop %>&pageIndex=<%=(pageIndex-1)%>&urlFilter=<%=urlFilter%>" > <%=prevPageCaption%></a> <% 
        }
       %>
       </td><td>
            <a href="index.jsp?status=crawlstatus&statusdrop=<%=statusdrop %>&pageIndex=<%=pageIndex+1 %>&urlFilter=<%=urlFilter%>"><%=nextPageCaption%></a>
       </td></tr>
       
       <script src="http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js"></script>
       <script>
            function deleteUrl(node){
                urltodelete = $.trim( $("td:first",node)[0].textContent );
                $(node).addClass("to_delete");
                var baseUrl = "index.jsp?status=crawlstatus&statusdrop=<%=statusdrop %>&pageIndex=<%=pageIndex %>&urlFilter=<%=urlFilter%>";
                document.location.replace(baseUrl + "&urltodelete=" + escape(urltodelete));
                
//                $.post("./", {
//                    urltodelete: url
//                },
//                function(data, textStatus) {
//                    document.location.replace("index.jsp?status=crawlstatus&statusdrop=<%=statusdrop %>&pageIndex=<%=pageIndex %>&urlFilter=<%=urlFilter%>");
//                    /*
//                    data = eval(data);
//                    $("#domainsToDeleteCount").text( "" + data.urlsToDelete + " domaines" );
//                    $("#urlTable tr > td").css("background-color", "red");*/
//                }, "json");
//                    
            }
      </script>
       
    </body>
</html>

<%@ page import="org.apache.nutch.admin.*"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>
<%@ page import="org.apache.hadoop.fs.*"%>
<%@ page import="org.apache.hadoop.io.*"%>
<%@ page import="org.apache.nutch.admin.crawldb.*"%>
<%@ page import="java.io.File"%>
<%@ page import="java.util.List"%>
<%@ page import="java.util.Set"%>
<%@ page import="java.util.Map"%>
<%@ page import="java.util.Date"%>
<%@ page import="java.util.Iterator"%>
<%@ page import="java.util.LinkedList"%>
<%@ page import="org.apache.nutch.crawl.*"%>

<html>
    <head>
        <title>Nutch Crawldb Statistic</title>
        <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
        <link rel="stylesheet" href="css/admingui.css" type="text/css">
    </head>
    <body bgcolor="#FFFFFF" text="#000000">

<%

GuiComponent component = (GuiComponent) application.getAttribute("component");
Path instanceFolder = component.getNutchInstance().getInstanceFolder();
Configuration configuration = component.getNutchInstance().getConfiguration();


String statusdrop = request.getParameter("statusdrop");
boolean reset = "false".equals(request.getParameter("reset")) ? false : true;
String statusMenu = request.getParameter("status");
String urlFilter = request.getParameter("urlFilter");
int pageIndex;
String pageIndexName = request.getParameter("pageIndex");
if ( pageIndexName == null || "".equals( pageIndexName ) ){
    pageIndex = 0;
}else{
    pageIndex = Integer.parseInt( pageIndexName );
}
String url = request.getParameter("url") != null ? request.getParameter("url") : "";

System.out.println("statusMenu  : " + statusMenu);
System.out.println("pageIndex : " + pageIndex);
System.out.println("urlFilter : " + urlFilter);

Map map = null;

if("crawlstatus".equals(statusMenu)) {
    Path crawldb = new Path(instanceFolder, "crawldb");
    UrlWithStatus dbStatistic = UrlWithStatus.getInstance(crawldb, configuration, reset);
    if( "all".equals( statusdrop ) ){
        map = dbStatistic.readUrl( pageIndex, urlFilter, false, (byte)0 );  
    }else{
        map = dbStatistic.readUrl( pageIndex, urlFilter, true, Byte.parseByte(statusdrop) );  
    }
}

//HttpSession session = request.getSession(true);


%>
        
        <form method="post" action="index.jsp">
            <table width="90%" border="0" class="table" align="center">
                <tr>
                    <td colspan="4" class="tableHead"><%=component.getLabel("crawldb.url.filters", request.getLocale())%></td>
                    <input type='hidden' name="status" value='crawlstatus' /> 
                </tr>
                <tr class="tableCellGrey">
                    <td width="20">
                        <%=component.getLabel("crawldb.url.urlfilter", request.getLocale())%>
                    </td>
                    <td width="300">
                        <input name="url" size="80" maxlength="128" type="text">
                    </td>
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
                                out.println("<option value=" + b + ">" + CrawlDatum.getStatusName(b) + "</option>");
                            }
                        %>
                        </select>
                    </td>
                </tr>
                <tr>
                    <td class="tableCellWhite">
                        <input type="submit" value="<%=component.getLabel("submit.button", request.getLocale())%>"/>
                    </td>
                </tr>
            </table>
        </form>
    
    <hr />
    <% 

            if("crawlstatus".equals(statusMenu)) {
                // Tableau des urls
                
                out.println("<table width='100%'>");
                
                out.println("<tr>");
                out.print("<td><b>" + "Url" + "</b></td>");
                out.print("<td><b>" + "Score" + "</b></td>");
                out.print("<td><b>" + "Status" + "</b></td>");
                out.print("<td><b>" + "Fetch time" + "</b></td>");
                out.print("<td><b>" + "Modified time" + "</b></td>");
                
                
                out.print("<td><b>" + "Retries since fetch" + "</b></td>");
                out.print("<td><b>" + "Retry interval" + "</b></td>");
                out.print("<td><b>" + "Metadata" + "</b></td>");
                
                out.println("</tr>");
                
                Set keys = map.keySet();
                Iterator iter = keys.iterator();
                while(iter.hasNext()) {
                    out.println("<tr>");
                    /*String[] status = (String[]) iter.next();
                    for(int i=0; i<status.length; i++) {
                      out.println("<div class="tableCellGrey">");
                      out.println(status[i]);
                      out.println("</div>"
                    }*/
                    String key = (String)iter.next();
                    out.print("<td><a href='.?status=crawlurl&url=" + key + "'>"  + key + "</a></td>");
                    CrawlDatum cd = (CrawlDatum) map.get(key); 
                    out.print("<td>" + cd.getScore() + "</td>");
                    out.print("<td>" + cd.getStatus() + " (" + cd.getStatusName(cd.getStatus()) + ") </td>");
                    out.print("<td>" + new Date(cd.getFetchTime()).toLocaleString() + "</td>");
                    out.print("<td>" + new Date(cd.getModifiedTime()).toLocaleString() + "</td>");
                    
                    out.print("<td>" + cd.getRetriesSinceFetch() + "</td>");
                    
                    
                    out.print("<td>" + (cd.getFetchInterval() / FetchSchedule.SECONDS_PER_DAY) + " days) </td>");
                    
                    out.print("<td>");
                    for (Map.Entry<Writable, Writable> e : cd.getMetaData().entrySet()) {
                        out.print(e.getKey());
                        out.print(": ");
                        out.print(e.getValue());
                        out.print("<br/>");
                      }

                    
                    // buf.append("Signature: " + StringUtil.toHexString(getSignature()) + "\n");
                    
                    
                    out.println("</tr>");                    
                }
                
                String prevPageCaption = component.getLabel("crawldb.previous", request.getLocale());
                String nextPageCaption = component.getLabel("crawldb.next", request.getLocale());
                // Liens pages precedente / suivante
                out.println("<tr><td>");
                out.println("<a href='index.jsp?status=crawlstatus&statusdrop=" + statusdrop + "&reset=false' >" + prevPageCaption + "</a>");
                out.println("</td><td>");
                out.println("<a href='index.jsp?status=crawlstatus&statusdrop=" + statusdrop + "&reset=false' >" + nextPageCaption + "</a>");
                out.println("</td></tr>");
                out.println("</table>");
                
        }
    %>
    </body>
</html>

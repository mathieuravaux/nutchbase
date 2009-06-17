<%@ page import="org.apache.nutch.crawl.*"%>
<%@ page import="org.apache.nutch.admin.*"%>
<%@ page import="org.apache.hadoop.conf.Configuration"%>
<%@ page import="org.apache.hadoop.fs.*"%>
<%@ page import="org.apache.hadoop.io.*"%>
<%@ page import="java.io.*"%>
<%@ page import="java.util.*"%>
<%@ page import="java.net.URL"%>
<%@ page import="org.apache.nutch.metadata.Nutch"%>
<%@ page import="org.apache.nutch.searcher.*"%>
<%@page import="java.text.DecimalFormat"%>

<%
GuiComponent component = (GuiComponent) application.getAttribute("component");

//Map map = (Map)request.getAttribute("map");

String query = (String)request.getParameter("query");
if(query == null){
    query = "";
}

List<HitDetails> results = (List<HitDetails>)request.getAttribute("results");
List<Summary> summaries = (List<Summary>)request.getAttribute("summaries"); 
ArrayList explanations = (ArrayList)request.getAttribute("explanations");
ArrayList pageranks = (ArrayList)request.getAttribute("pageranks");
ArrayList votes = (ArrayList)request.getAttribute("votes");

%>

<html>
    <head>
        <title>Nutch Scores</title>
        <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1">
        <link rel="stylesheet" href="css/admingui.css" type="text/css">
    </head>
    <body bgcolor="#FFFFFF" text="#000000">
        <form method="post" action="index.jsp">
            <p width="100%" border="0">
                Rechercher : <input name="query" size="80" maxlength="100" type="text" value="<%=query%>">
                <input type="submit" value="<%=component.getLabel("submit.button", request.getLocale())%>"/>
            </p>
        </form>
      
    <table width="100%" id="urlTable" cellpadding=0 cellspacing=0>
    <tr>
        <td class="tableHead">?</td>
        <td class="tableHead">Titre</td>
        <td class="tableHead">Url</td>
        <td class="tableHead">Résumé</td>
        <td class="tableHead" width="36px">Boost</td>
        <td class="tableHead" width="110px">Pagerank</td>
        <td class="tableHead" width="110px">Votes</td>
    </tr>
    
    <% 
       if(results != null){
        DecimalFormat scoreFormatter = new DecimalFormat("##0.00");

        for(int i=0; i < results.size(); i++){
            HitDetails detail = results.get(i);
            String title = detail.getValue("title");
            String url = detail.getValue("url");
            String summary = summaries.get(i).toHtml(true);
            String explanation = (String)explanations.get(i);
            float pagerank = (Float)pageranks.get(i);
            float nb_votes = (Float)votes.get(i);
            float boost = Float.valueOf(detail.getValue("boost"));
           
         %>
         <tr>
            <td style="position:relative"><img src="img/info.png" class="infobutton"> <div class="explanation"><%=explanation%></div></td>
            <td><%=title%></td>
            <td><a class="url" href="<%=url%>" target="_blank"><%=url%></a></td>
            <td><%=summary%></td>
            <td class="boost"><%=scoreFormatter.format(boost) %></td>
            <td class="pagerank"><%=scoreFormatter.format(pagerank) %></td>
            <td class="nb_votes"><%=scoreFormatter.format(nb_votes) %></td>
         </tr>
      <%                    
        }}else {
          out.println("<pre>Results was null !</pre>");  
        }
      %> 
       
       <div id="form-template" style="display:none">
  <!-- start form -->
  <div class="in-place-edit">
    <input type="text" class="field"></input>
    <div class="buttons">
      <input type="button" value="OK" class="save-button" />
      <input type="button" value="Annuler" class="cancel-button" />
    </div>
  </div>
  <!-- end form -->
</div>
       
       <script src="http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js"></script>
       <script>


       (function($) {

          $.fn.inPlaceEdit = function(options) {

          // Add click handler to all matching elements
          return this.each(function() {
            // Use default options, if necessary
            var settings = $.extend({}, $.fn.inPlaceEdit.defaults, options);

            var element = $(this);
            
            element.click(function() {
              // Prevent multiple clicks, and check if inplace editing is disabled
              if (element.hasClass("editing") || element.hasClass("disabled")) {
                  return;
              }

              element.addClass("editing");

              element.old_html = element.html();          // Store old HTML so we can revert to it later
              
              if(typeof(settings.html) == 'string') {     // There are two types of form templates: strings and DOM elements
                element.html(settings.html);              // Replace current HTML with given HTML
              }
              else {
                element.html('');                         // Replace current HTML with given object's HTML
                var form_template = settings.html.children(':first').clone(true);
                form_template.appendTo(element);          // Clone event handlers too
              }

              $('.field', element).focus();               // Set focus to input field
              $('.field', element).select();              // Select all text in field
              $('.field', element).val(element.old_html); // Set field value to old HTML
              
              // On blur: cancel action
              if(settings.onBlurDisabled == false) {
                $('.field', element).blur(function() {
                  // Prevent cancel from being triggered when clicking Save button
                  element.timeout = setTimeout(cancel, 500);
                });
              }
              
              // On save: revert to old HTML and submit
              $('.save-button', element).click(function() {
                return submit();
              });

              // On cancel: revert to old HTML
              $('.cancel-button', element).click(function() {
                return cancel();
              });
              
              // On keyup: submit (ESC) or cancel (enter)
              if(settings.onKeyupDisabled == false) {
                $('.field', element).keyup(function(event) {
                  var keycode = event.which;
                  var type = this.tagName.toLowerCase();
                  
                  if(keycode == 27 && settings.escapeKeyDisabled == false)    {        // escape
                    return cancel();
                  } 
                  else if(keycode == 13) { // enter
                    // Don't submit on enter if this is a textarea
                    if(type != "textarea") {
                      return submit();
                    }
                  }
                  return true;
                });
              }
            });
            
            // Add hover class on mouseover
            element.mouseover(function() {
              element.addClass("hover");
            });
            
            // Remove hover class on mouseout
            element.mouseout(function() {
              element.removeClass("hover");
            });
            
            function cancel() {
              element.html(element.old_html);

              element.removeClass("hover editing");

              if(options.cancel) {
                options.cancel.apply(element, [element]);
              }
              return false; // Stop propagation
            };
            
            function submit() {
              clearTimeout(element.timeout);

              var id = element.attr('id');
              var value = $('.field', element).val();

              if(options.submit) {
                options.submit.apply(element, [element, id, value]);
              }
              
              element.removeClass("hover editing");

              element.html(value);
              
              return false; // Stop propagation
            };
          });
          
        };
        
        // Default (overridable) settings
        $.fn.inPlaceEdit.defaults = {
          onBlurDisabled  : false,
          onKeyupDisabled : false,
          escapeKeyDisabled : false,
          html : ' \
                <div class="inplace-edit"> \
                  <input type="text" value="" class="field" /> \
                  <div class="buttons"> \
                    <input type="button" value="Save" class="save-button" /> \
                    <input type="button" value="Cancel" class="cancel-button" /> \
                  </div> \
                </div>'
        };
      })(jQuery);

       
       
        $(function(){
            $(".infobutton").live("mouseover", function(){ $(this).next().show(); });
            $(".infobutton").live("mouseout", function(){ $(this).next().hide(); });

            var submit_pagerank = function(td, id, value) {
                var url = $(td).siblings().children(".url").text();
                var pagerank = parseFloat($.trim(value).replace(",", "."));
                //console.info("edited pagerank for", url, ":", pagerank);
                $.post("modify_url/", {
                    url: url,
                    pagerank: pagerank
                },
                function(data, textStatus) {},
                "json");
                return true;
             };

             var submit_votes = function(td, id, value) {
                  var url = $(td).siblings().children(".url").text();
                  var votes = parseFloat($.trim(value).replace(",", "."));
                  //console.info("edited votes for", url, ":", votes);
                  $.post("modify_url/", {
                	      url: url,
                          votes: votes
                      },
                      function(data, textStatus) {},
                      "json");
                  return true;
             };

             var cancel_handler = function(element) {
                // Nothing
                return true;
              };

              $(".pagerank").inPlaceEdit({
                submit : submit_pagerank,
                cancel : cancel_handler,
                html   : $('#form-template')
              });
              $(".nb_votes").inPlaceEdit({
                  submit : submit_votes,
                  cancel : cancel_handler,
                  html   : $('#form-template')
              });
                
            
        });
            
       </script>
       
    </body>
</html>

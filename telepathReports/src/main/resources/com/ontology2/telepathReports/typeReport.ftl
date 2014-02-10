<html>
 <head>
  <title>Alive and Kicking</title>
 </head>
 <body>
  <table>
    <#list rows as row >
    <tr><td>${row.type}</td><td>${row.sum}</td><td>${row.cnt}</td></tr>
    </#list>
  <table>
 </body>
</html>
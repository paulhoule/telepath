<html>
 <head>
  <title>Alive and Kicking</title>
 </head>
 <body>
  <table>
    <tr><th>#</th><th>type</th><th>total importance</th><th>instance count</th>
    <#list rows as row >
    <tr><td>${row.i}</td><td><a href="${row.typeLink}">${row.shortType}</a></td><td>${row.sum}</td><td>${row.cnt}</td></tr>
    </#list>
  <table>
 </body>
</html>
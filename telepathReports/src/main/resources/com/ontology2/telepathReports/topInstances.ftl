<html>
 <head>
  <title>Alive and Kicking</title>
 </head>
 <body>
  <table>
    <tr><th>#</th>type</th><th>total importance</th><th>instance count</th>
    <#list rows as row >
    <tr><td>${row.i}</td><td>${row.s}</td><td>${row.eye}</td></tr>
    </#list>
  <table>
 </body>
</html>
<html>
 <head>
  <title>Alive and Kicking</title>
 </head>
 <body>
  <table>
    <tr><th>#</th><th>type</th><th>total importance</th><th>instance count</th>
    <#list rows as row >
    <tr>
      <td>${row.i}</td>
      <td><a href="${row.mappingWikiLink}">${row.shortType}</a> [<a href="${row.typeLink}">detail</a>]</td>
      <td>${row.sum?string("0.##E0")}</td>
      <td>${row.cnt}</td></tr>
    </#list>
  <table>
 </body>
</html>
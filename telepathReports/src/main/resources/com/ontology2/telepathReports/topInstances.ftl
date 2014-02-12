  <table>
    <tr><th>#</th><th>instance</th><th>importance</th><th>percentage of total importance</th>
    <#list rows as row >
    <tr>
      <td>${row.i}</td>
      <td><a href="${row.wikipediaLink}">${row.shortSubject}</a></td>
      <td>${row.eye?string("0.##E0")}</td>
      <td>${row.eyePercentage?string("0.##E0")}</td>
    </tr>
    </#list>
  <table>

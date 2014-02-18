  <h1>top instances of type ${shortType}</h1>
  <p>
  Column 1: ranking of type within list<br>
  Column 2: instance object (linked to Wikipedia)<br>
  Column 3: importance score for instance<br>
  Column 4: 100.0 * (importance of this instance)/(importance of all instances of this type)<br>
  </p>
  ${scoreExplanation}
  <p>
  <a href="${otherLink}">Click here</a> to see a similar report generated for ${otherName} importance, or
  <a href="typeReport.html">here</a> to see types ranked by importance
  </p>
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

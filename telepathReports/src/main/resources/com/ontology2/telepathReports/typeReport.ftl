  <h1>Types ranked by cumulative importance</h1>
  <p>
  <a href="http://wiki.dbpedia.org/Ontology">Dbpedia Ontology</a> types ranked according to subjective importance.  The
  first column is ranked order in terms of total importance,  the second column is the type,  the third column is the sum
  of the importance of all instances and the fourth column is the total number of instances of this type.
  </p>
  ${scoreExplanation}
  <p>
  <a href="${otherLink}">Click here</a> to see a similar report generated for ${otherName} importance.
  </p>
  <table>
    <tr><th>#</th><th>type</th><th>total importance</th><th>instance count</th>
    <#list rows as row >
    <tr>
      <td>${row.i}</td>
      <td><a href="${row.typeLink}">${row.shortType}</a> [<a href="${row.mappingWikiLink}">concept definition</a>]</td>
      <td>${row.sum?string("0.##E0")}</td>
      <td>${row.cnt}</td></tr>
    </#list>
  <table>


function telepathReports {
   java -jar $env:userprofile\.m2\repository\com\\ontology2\telepathReports\${project.version}\telepathReports-${project.version}-onejar.jar @args
}
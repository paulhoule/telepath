package com.ontology2.telepathReports;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.hp.hpl.jena.query.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component("sparqlService")
public class SparqlService {
    @Autowired
    ApplicationContext applicationContext;

    public final String service="http://localhost:8890/sparql";

    public Query getQuery(String queryName) throws IOException {
        return getQuery(queryName,new QuerySolutionMap());
    }

    public Query getQuery(String queryName,QuerySolutionMap map) throws IOException {
        Resource r= new ClassPathResource(queryName);
        String queryText= Resources.toString(r.getURL(), Charsets.UTF_8);
        ParameterizedSparqlString pss=new ParameterizedSparqlString(queryText,map);
        return QueryFactory.create(queryText);
    }

    public ResultSet select(Query q) {
        QueryExecution that=QueryExecutionFactory.sparqlService(service, q);
        that.setTimeout(0);
        return that.execSelect();
    }
}

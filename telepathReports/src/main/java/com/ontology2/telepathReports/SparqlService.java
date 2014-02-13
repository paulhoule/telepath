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

public class SparqlService {
    @Autowired
    ApplicationContext applicationContext;
    final String service;

    public SparqlService(String service) {
        this.service=service;
    }

    public Query getQuery(String queryName) throws IOException {
        return getQuery(queryName,new QuerySolutionMap());
    }

    public Query getQuery(String queryName,QuerySolutionMap map) throws IOException {
        Resource r= new ClassPathResource(queryName);
        String queryText= Resources.toString(r.getURL(), Charsets.UTF_8);
        ParameterizedSparqlString pss=new ParameterizedSparqlString(queryText,map);
        return pss.asQuery();
    }

    public ResultSet select(Query q) {
        QueryExecution that=QueryExecutionFactory.sparqlService(service, q);
        that.setTimeout(0);
        return that.execSelect();
    }
}

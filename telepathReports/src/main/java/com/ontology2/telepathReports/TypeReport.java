package com.ontology2.telepathReports;

import com.google.common.base.Charsets;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import com.hp.hpl.jena.query.*;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Component("typeReport")
public class TypeReport extends CommandLineApplication {
    @Autowired
    ApplicationContext applicationContext;

    @Override
    protected void _run(String[] strings) throws Exception {
        String service="http://localhost:8890/sparql";

        Resource r= applicationContext.getResource("/com/ontology2/telepathReports/typeReport.sparql");
        String queryText= Resources.toString(r.getURL(),Charsets.UTF_8);
        Query query=QueryFactory.create(queryText);
        QueryExecution qe= QueryExecutionFactory.sparqlService(service, query);
        qe.setTimeout(0);
        ResultSet results=qe.execSelect();
        while(results.hasNext()) {
            QuerySolution row=results.next();
            StringBuffer out=new StringBuffer();
            Iterator<String> names=row.varNames();
            int cnt=0;
            while(names.hasNext()) {
                if(cnt>0)
                    out.append(",");

                out.append(row.get(names.next()));
                cnt++;
            }
            System.out.println(out);
        }
    }
}

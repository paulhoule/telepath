package com.ontology2.telepathReports;

import com.hp.hpl.jena.query.*;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Iterator;


@Component("typeReport")
public class TypeReport extends CommandLineApplication {
    @Autowired
    SparqlService sparqlService;

    @Override
    protected void _run(String[] strings) throws Exception {

        Query query=sparqlService.getQuery("/com/ontology2/telepathReports/typeReport.sparql");
        ResultSet results=sparqlService.select(query);
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

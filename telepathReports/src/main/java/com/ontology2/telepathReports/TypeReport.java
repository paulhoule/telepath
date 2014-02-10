package com.ontology2.telepathReports;

import com.hp.hpl.jena.query.*;
import com.ontology2.centipede.shell.CommandLineApplication;
import freemarker.template.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;


@Component("typeReport")
public class TypeReport extends CommandLineApplication {
    @Autowired
    SparqlService sparqlService;

    @Override
    protected void _run(String[] strings) throws Exception {
        Query query=sparqlService.getQuery("/com/ontology2/telepathReports/typeReport.sparql");
        ResultSet results=sparqlService.select(query);

        List<Map<String,Object>> rowz=newArrayList();
        while(results.hasNext()) {
            QuerySolution row=results.next();
            Iterator<String> names=row.varNames();
            Map<String,Object> rowOut=newHashMap();
            rowz.add(rowOut);
            while(names.hasNext()) {
                String name=names.next();
                rowOut.put(name, row.get(name));
            }
        }

        Map<String,Object> root=newHashMap();
        root.put("rows",rowz);
        Configuration c=new Configuration();
        c.setClassForTemplateLoading(SparqlService.class,"/com/ontology2/telepathReports");
        c.setObjectWrapper(new DefaultObjectWrapper());
        c.setDefaultEncoding("UTF-8");
        c.setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER);
        c.setIncompatibleImprovements(new Version(2,3,20));

        Template t=c.getTemplate("typeReport.ftl");
        Writer out=new OutputStreamWriter(System.out);
        t.process(root,out);
    }

}

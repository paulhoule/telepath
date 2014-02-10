package com.ontology2.telepathReports;

import com.hp.hpl.jena.query.*;
import com.ontology2.centipede.shell.CommandLineApplication;
import freemarker.template.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;


@Component("typeReport")
public class TypeReport extends CommandLineApplication {
    @Autowired SparqlService sparqlService;
    @Autowired TemplateService templateService;
    @Autowired MaterializationService materializationService;

    @Override
    protected void _run(String[] strings) throws Exception {
        String webBase="C:\\webbase\\";
        String webPrefix="reports";
        String reportType="typeReport";
        String guid= UUID.randomUUID().toString();

        Query query=sparqlService.getQuery("/com/ontology2/telepathReports/typeReport.sparql");
        ResultSet results=sparqlService.select(query);
        Map<String,Object> model=materializationService.createModel(results);
        FluentFile baseFile=new FluentFile(webBase).slash(webPrefix).slash(reportType).slash(guid);
        FluentFile outfile=baseFile.slash("typeReport.html");
        outfile.ensureDir();
        templateService.template("typeReport.ftl", model, outfile.writer());
        System.out.println(outfile.toURI());
    }
}

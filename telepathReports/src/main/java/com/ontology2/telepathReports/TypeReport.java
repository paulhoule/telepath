package com.ontology2.telepathReports;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.ontology2.centipede.shell.CommandLineApplication;
import freemarker.template.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;

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

        URIRewriter rewriter=new URIRewriter();
        rewriter.shorten("type","shortType");
        rewriter.rebase("type", "typeLink", ".html", new HashMap() {{
            put("ontology:","./");
        }} );

        for(Map<String,Object> row: extractRows(model)) {
            rewriter.enrichRow(row);
        }

        FluentFile baseFile=new FluentFile(webBase).slash(webPrefix).slash(reportType).slash(guid);
        FluentFile outfile=baseFile.slash("typeReport.html");
        outfile.ensureDir();
        model.put("title","Types in the DBpedia Ontology sorted by aggregate importance");
        templateService.template("typeReport.ftl", model, outfile.writer());

        for(Map<String,Object> row: extractRows(model)) {
            generateInstanceReport(baseFile, row);
        }
        
        System.out.println(outfile.toURI());
    }

    private void generateInstanceReport(FluentFile baseFile, final Map<String, Object> row) throws IOException, TemplateException {
        String type= Iterables.getLast(Splitter.on(":").split(row.get("shortType").toString()));
        Query query=sparqlService.getQuery("/com/ontology2/telepathReports/topInstances.sparql",new QuerySolutionMap() {{
            add("type", (RDFNode) row.get("type"));
        }});
        ResultSet results=sparqlService.select(query);
        Map<String,Object> model=materializationService.createModel(results);
        FluentFile outfile=baseFile.slash(type+".html");
        outfile.ensureDir();

        model.put("title","Most important instances of type :"+type);
        templateService.template("topInstances.ftl", model, outfile.writer());
    }

    private List<Map<String, Object>> extractRows(Map<String, Object> model) {
        return (List<Map<String,Object>>) model.get("rows");
    }
}

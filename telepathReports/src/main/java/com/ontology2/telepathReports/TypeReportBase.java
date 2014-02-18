package com.ontology2.telepathReports;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.sparql.util.NodeFactoryExtra;
import com.ontology2.centipede.shell.CommandLineApplication;
import freemarker.template.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;


// @Component("typeReport")
abstract public class TypeReportBase extends CommandLineApplication {
    @Autowired SparqlService sparqlService;
    @Autowired TemplateService templateService;
    @Autowired MaterializationService materializationService;
    @Autowired String webBase;
    double conversionFactor=4.323E-5F;

    protected TypeReportBase() {
        this.guid = UUID.randomUUID().toString();
    }
    protected TypeReportBase(String guid) {
        this.guid = guid;
    }

    abstract protected String getVariant();
    abstract protected String getQueryName();
    abstract protected String getInstanceQueryName();
    abstract protected String getScoreExplanation();
    abstract protected String otherBase();
    abstract protected String otherName();

    protected final String guid;

    @Override
    protected void _run(String[] strings) throws Exception {
        String webPrefix="reports";
        String reportType="typeReport";

        final Model m= ModelFactory.createDefaultModel();
        Query query=sparqlService.getQuery(getQueryName(),new QuerySolutionMap() {{
            add("conversionFactor", m.asRDFNode(NodeFactoryExtra.doubleToNode(conversionFactor)));
        }});
        ResultSet results=sparqlService.select(query);
        Map<String,Object> model=materializationService.createModel(results);

        URIRewriter rewriter=new URIRewriter();
        rewriter.shorten("type","shortType");
        rewriter.rebase("type", "typeLink", ".html", new HashMap() {{
            put("ontology:","./");
        }} );
        rewriter.rebase("type", "mappingWikiLink", "", new HashMap() {{
            put("ontology:","http://mappings.dbpedia.org/server/ontology/classes/");
        }} );

        rewriter.enrichModel(model);

        FluentFile baseFile=new FluentFile(webBase).slash(webPrefix).slash(guid).slash(reportType).slash(getVariant());
        FluentFile outfile=baseFile.slash("typeReport.html");
        outfile.ensureDir();
        model.put("title", "Types in the DBpedia Ontology sorted by aggregate importance");
        model.put("scoreExplanation",templateService.subTemplate(getScoreExplanation()));
        model.put("otherBase",otherBase());
        model.put("otherLink",otherBase()+"/typeReport.html");
        model.put("otherName",otherName());

        templateService.template("typeReport.ftl", model, outfile.writer());

        for(Map<String,Object> row: extractRows(model)) {
            generateInstanceReport(baseFile, row);
        }
        
        System.out.println(outfile.toURI());
    }

    private void generateInstanceReport(FluentFile baseFile, final Map<String, Object> typeRow) throws IOException, TemplateException {
        String type= Iterables.getLast(Splitter.on(":").split(typeRow.get("shortType").toString()));
        final double normalizationFactor=100.0/(Double) typeRow.get("sum");

        final Model m= ModelFactory.createDefaultModel();
        Query query=sparqlService.getQuery(getInstanceQueryName(),new QuerySolutionMap() {{
            add("type", (RDFNode) typeRow.get("type"));
            add("conversionFactor", m.asRDFNode(NodeFactoryExtra.doubleToNode(conversionFactor)));
        }});
        ResultSet results=sparqlService.select(query);
        Map<String,Object> model=materializationService.createModel(results);
        model.put("shortType",typeRow.get("shortType"));
        model.put("typeLink",typeRow.get("typeLink"));
        model.put("otherLink",otherBase()+"/"+type+".html");
        model.put("otherName",otherName());

        URIRewriter rewriter=new URIRewriter();
        rewriter.shorten("s","shortSubject");
        rewriter.rebase("s", "wikipediaLink", "", new HashMap() {{
            put("dbpedia:", "http://en.wikipedia.org/wiki/");
        }});

        rewriter.applyFn("eye", "eyePercentage", new Function<Double, Object>() {
            @Override
            public Object apply(@Nullable Double aFloat) {
                return aFloat*normalizationFactor;
            }
        });
        rewriter.enrichModel(model);

        FluentFile outfile=baseFile.slash(type+".html");
        outfile.ensureDir();

        model.put("title","Most important instances of type :"+type);
        model.put("scoreExplanation",templateService.subTemplate(getScoreExplanation()));
        templateService.template("topInstances.ftl", model, outfile.writer());
    }



    private List<Map<String, Object>> extractRows(Map<String, Object> model) {
        return (List<Map<String,Object>>) model.get("rows");
    }
}

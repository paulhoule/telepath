package com.ontology2.telepathReports;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.QuerySolutionMap;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.util.NodeFactoryExtra;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import java.io.PrintStream;
import java.nio.charset.Charset;

import static com.google.common.collect.Iterables.getLast;

@Component("outputPairedImportance")
public class OutputPairedImportance extends CommandLineApplication {
    @Autowired
    SparqlService sparqlService;

    Splitter slashSplitter= Splitter.on("/");
    final String baseUri="http://dbpedia.org/resource/";

    final int BLOCKSIZE=500000;

    @Override
    protected void _run(String[] strings) throws Exception {
        Query q1=sparqlService.getQuery("com/ontology2/telepathReports/pairedImportanceCount.sparql");
        ResultSet rs2=sparqlService.select(q1);
        int rowCount=rs2.next().getLiteral("cnt").getInt();
        int j=0;

        for(int i=0;i<rowCount;i+=BLOCKSIZE) {
            final Model m= ModelFactory.createDefaultModel();
            int limit=rowCount-BLOCKSIZE;
            limit = limit>BLOCKSIZE ? BLOCKSIZE : limit;
            final int ii=i;
            final int llimit=limit;

            Query q=sparqlService.getQuery("com/ontology2/telepathReports/pairedImportance.sparql",new QuerySolutionMap() {{
                add("o",m.asRDFNode(NodeFactoryExtra.intToNode(ii)));
                add("l",m.asRDFNode(NodeFactoryExtra.intToNode(llimit)));
            }});
            ResultSet results=sparqlService.select(q);

            while(results.hasNext()) {
                QuerySolution that=results.next();
                j++;
                String uri=that.get("s").toString();
                String eye=Float.toString(that.get("eye").asLiteral().getFloat());
                String pr=Float.toString(that.get("pr").asLiteral().getFloat());


                String shortUri=uri.substring(baseUri.length());
                System.out.print(shortUri);
                System.out.print("\t");
                System.out.print(eye);
                System.out.print("\t");
                System.out.print(pr);
                System.out.println();
            }
        }

        if (j!=rowCount) {
            throw new Exception("Expected to get "+rowCount+" results but really got "+j+" results");
        }
    }
}

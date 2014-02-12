package com.ontology2.telepathReports;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import org.springframework.context.annotation.Configuration;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

@Configuration("materializationService")
public class MaterializationService {
    public List<Map<String, Object>> materializeResultSet(ResultSet results) {
        List<Map<String,Object>> rowz=newArrayList();
        int i=1;

        while(results.hasNext()) {
            QuerySolution row=results.next();
            Iterator<String> names=row.varNames();
            Map<String,Object> rowOut=newHashMap();
            rowz.add(rowOut);
            while(names.hasNext()) {
                String name=names.next();
                rowOut.put(name, flattenNode(row.get(name)));
            }
            rowOut.put("i", i++);
        }
        return rowz;
    }

    public Map<String,Object> createModel(ResultSet results) {
        Map<String,Object> root=newHashMap();
        root.put("rows",materializeResultSet(results));
        return root;
    }

    public Object flattenNode(Object o) {
        if(!(o instanceof Literal))
            return o;

        Literal that=(Literal) o;
        RDFDatatype t=that.getDatatype();
        if (t== XSDDatatype.XSDfloat)
            return that.getFloat();

        if (t== XSDDatatype.XSDdouble)
            return that.getDouble();

        if (t== XSDDatatype.XSDinteger || t==XSDDatatype.XSDint)
            return that.getInt();

        if (t==XSDDatatype.XSDlong)
            return that.getLong();

        if (t==XSDDatatype.XSDshort)
            return that.getShort();

        if (t==XSDDatatype.XSDboolean)
            return that.getBoolean();

        if (t==XSDDatatype.XSDstring)
            return that.getString();

        // XXX -- it would be nice to have support for decimal,  datetime,  etc.
        // one of the harder things to what to do about language tags...  in some cases we want to deep
        // six them,  other times we want to keep them

        return that;
    }
}

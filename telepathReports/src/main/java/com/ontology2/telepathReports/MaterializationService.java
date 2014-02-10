package com.ontology2.telepathReports;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
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
        return rowz;
    }

    public Map<String,Object> createModel(ResultSet results) {
        Map<String,Object> root=newHashMap();
        root.put("rows",materializeResultSet(results));
        return root;
    }
}

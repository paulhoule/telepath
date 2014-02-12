package com.ontology2.telepathReports;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.graph.Node_URI;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.PrefixMapping;
import org.apache.jena.riot.system.PrefixMap;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;

//
// Note that this is definitely NOT packaged as a Spring-configured `Service` because it will
// be configured again and again inside the same application for different reports
//

public class URIRewriter {
    PrefixMapping mapping;
    Multimap<String,Rule> linkMap;   // sourceField -> (destinationField, function)

    {
        mapping=new PrefixMapping.Factory().create();
        // XXX -- make easily configurable
        mapping.setNsPrefix("ontology","http://dbpedia.org/ontology/");
        mapping.setNsPrefix("dbpedia","http://dbpedia.org/resource/");
        linkMap= ArrayListMultimap.create();
    }

    public void enrichRow(Map<String,Object> row) {
        List<String> rowKeys=newArrayList(row.keySet());
        for(String key:rowKeys) {
            if(linkMap.containsKey(key)) {
                for(Rule that:linkMap.get(key))
                    row.put(that.getMapsTo(),that.getFn().apply(row.get(key)));
            }
        }
    }

    public void enrichModel(Map<String,Object> model) {
        enrichModel(model,"rows");
    }

    public void enrichModel(Map<String,Object> model,String rowSetName) {
        for(Map<String,Object> row: (List<Map<String,Object>>) model.get(rowSetName)) {
            enrichRow(row);
        }
    }

    public String rewrite(Object that) {
        if(that instanceof Node_URI) {
            return mapping.shortForm(that.toString());
        } if (that instanceof Resource) {
            return mapping.shortForm(((Resource) that).getURI());
        }
        return that.toString();
    }

    public class Rule {
        private final String mapsTo;
        private final Function<Object,Object> fn;

        public Rule(String mapsTo,Function<Object,Object> fn) {
            this.mapsTo=mapsTo;
            this.fn=fn;
        }

        private String getMapsTo() {
            return mapsTo;
        }

        private Function<Object,Object> getFn() {
            return fn;
        }
    }

    public void shorten(String fromKey,String toKey) {
        linkMap.put(fromKey,new Rule(toKey,new Function<Object,Object>() {
            @Override
            public String apply(@Nullable Object o) {
                return rewrite(o);
            }
        }));
    }

    //
    // if a URI (after shortening) starts with the key of theMap,  we replace the prefix
    // with the value in theMap
    //
    public void rebase(String fromKey,String toKey, final String extension,final Map<String,String> theMap) {
        linkMap.put(fromKey,new Rule(toKey,new Function<Object,Object>() {
            @Override
            public String apply(@Nullable Object o) {
                String shortURI=rewrite(o);
                for(Map.Entry<String,String> e:theMap.entrySet()) {
                    if(shortURI.startsWith(e.getKey()))
                        shortURI=e.getValue() + shortURI.substring(e.getKey().length()) + extension;
                }
                return shortURI;
            }
        }));
    }

    public <T> void applyFn(String fromKey,String toKey,final Function<T,Object> fn) {
        linkMap.put(fromKey,new Rule(toKey,new Function<Object,Object>() {
            @Override
            public Object apply(@Nullable Object o) {
                return fn.apply((T) o);
            }
        }));
    }
}

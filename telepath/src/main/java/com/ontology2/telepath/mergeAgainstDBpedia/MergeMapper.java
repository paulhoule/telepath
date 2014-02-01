package com.ontology2.telepath.mergeAgainstDBpedia;

import com.ontology2.bakemono.joins.GeneralTextJoinMapper;
import com.ontology2.bakemono.joins.TaggedTextItem;
import com.ontology2.bakemono.util.Utilities;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.google.common.collect.Maps.immutableEntry;
import static com.ontology2.bakemono.util.Utilities.WHITESPACE_SPLITTER;

public class MergeMapper extends GeneralTextJoinMapper {

    // tag 1 is a page id entry
    // tag 2 is a redirect entry
    // tag 3 is numeric data

    // the key is always http://lang.dbpedia.org/



    @Override
    public Map.Entry<Text, Text> splitValue(Writable writable, VIntWritable tag) {
        Text that=(Text) writable;
        int tagValue=tag.get();
        String line=that.toString();

        if(tagValue<3) {
            if(line.startsWith("#"))
                return null;

            String subject;
            String predicate;
            String object;

            Iterator<String> parts= WHITESPACE_SPLITTER.split(line).iterator();
            try {
                subject=parts.next();
                predicate=parts.next();
                object=parts.next();
            } catch(NoSuchElementException ex) {
                return null;
            }

            if (tagValue==1) {
                return immutableEntry(new Text(subject),new Text(""));
            } else if (tagValue==2) {
                return immutableEntry(new Text(subject),new Text(object));
            }
        }

        Iterator<String> parts= WHITESPACE_SPLITTER.split(line).iterator();
        String label;
        String name;
        String score;
        try {
            label=parts.next();
            name=parts.next();
            score=parts.next();
        } catch(NoSuchElementException ex) {
            return null;
        }

        if(!label.equals("en"))
            return null;

        StringBuilder uri=new StringBuilder();
        uri.append("<");
        uri.append("http://dbpedia.org/resource/");
        uri.append(name);
        uri.append(">");

        return immutableEntry(new Text(uri.toString()),new Text(score));
   }
}

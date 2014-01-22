package com.ontology2.telepath.topNForLang;


import com.google.common.base.Splitter;
import com.ontology2.bakemono.joins.TaggedTextItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class TopNForLangMapper extends Mapper<Text,Text,TaggedTextItem,TaggedTextItem> {
    Splitter SPACE_SPLITTER= Splitter.on(" ");

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Iterator<String> keyParts=SPACE_SPLITTER.split(key.toString()).iterator();
        try {
            String lang=keyParts.next();
            String uri=keyParts.next();
            int count=Integer.parseInt(value.toString());
            context.write(new TaggedTextItem(lang,count),new TaggedTextItem(uri,count));
        } catch(NoSuchElementException |NumberFormatException ex) {
            System.out.println(ex);
        }
    }
}

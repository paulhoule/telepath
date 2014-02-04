package com.ontology2.telepath.mergeAgainstDBpedia;

import com.google.common.base.Splitter;
import com.ontology2.bakemono.joins.TaggedTextItem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MergeReducer extends Reducer<TaggedTextItem,TaggedTextItem,Text,Text> {
    Log LOG= LogFactory.getLog(MergeReducer.class);

    @Override
    protected void reduce(TaggedTextItem key, Iterable<TaggedTextItem> values, Context context) throws IOException, InterruptedException {
        Text toValue=null;

        LOG.info("Processed key ["+key.getKey()+"] with tag ["+key.getTag().get()+"]");
        for(TaggedTextItem value:values) {
            LOG.info("Processed value ["+value.getKey()+"] with tag ["+value.getTag().get()+"]");
            int tag=value.getTag().get();
            switch(tag) {
                case 1:
                    toValue=key.getKey();      // we can write the key because it exists
                    break;
                case 2:
                    toValue=new Text(value.getKey());    // we will write the value because we redirect to it
                    break;
                case 3:
                    if(toValue!=null)
                    context.write(toValue,value.getKey());
            }
        }
    }
}

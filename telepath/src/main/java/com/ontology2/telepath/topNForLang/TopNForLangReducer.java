package com.ontology2.telepath.topNForLang;

import com.ontology2.bakemono.joins.TaggedTextItem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNForLangReducer extends Reducer<TaggedTextItem,TaggedTextItem,Text,Text> {
}

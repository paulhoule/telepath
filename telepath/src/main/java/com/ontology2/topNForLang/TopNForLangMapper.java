package com.ontology2.topNForLang;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNForLangMapper extends Mapper<Text,Text,LangCount,URICount> {
}

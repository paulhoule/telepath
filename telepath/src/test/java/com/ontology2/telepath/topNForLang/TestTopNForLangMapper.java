//package com.ontology2.telepath.topNForLang;
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.IOException;
//
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.verify;
//
//public class TestTopNForLangMapper {
//    TopNForLangMapper mapper;
//    Mapper<Text,Text,LangCount,WritableComparablePair>.Context context;
//
//    @Before
//    public void setup() {
//        mapper=new TopNForLangMapper();
//        context=mock(Mapper.Context.class);
//    }
//
//    @Test
//    public void tryASimpleOne() throws IOException, InterruptedException {
//        Text key=new Text("en And_You_And_I");
//        Text value=new Text("90125");
//        mapper.map(key,value,context);
//        verify(context).write(new LangCount("en",90125),new WritableComparablePair("And_You_And_I",90125));
//    }
//}

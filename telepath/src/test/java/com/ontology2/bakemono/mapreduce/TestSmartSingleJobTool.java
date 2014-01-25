package com.ontology2.bakemono.mapreduce;

import com.google.common.reflect.TypeToken;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static junit.framework.Assert.*;

public class TestSmartSingleJobTool {
    TestTool that;

    @Before
    public void setup() {
        that=new TestTool();
        that.setBeanName("wildIsTheWind");
    }

    @Test
    public void getTypeArguments() {
        // Creating an anonymous class reifies the type option
        List<String> target=new ArrayList<String>() {{
            add("thimble");
        }};

        Type[] arguments=SmartSingleJobTool.sniffTypeParameters(target.getClass(),ArrayList.class);
        assertEquals(1, arguments.length);
        assertEquals(String.class,arguments[0]);
    }

    @Test(expected=SmartSingleJobTool.NoGenericTypeInformationAvailable.class)
    public void detectCommonGenericsMistake() {
        List<String> target=newArrayList();
        Type[] arguments=SmartSingleJobTool.sniffTypeParameters(target.getClass(),ArrayList.class);
        assertEquals(1,arguments.length);
        assertEquals(String.class,arguments[0]);
    }

    @Test
    public void optionType() {
        assertEquals(TestOptions.class, that.getOptionsClass());
    }

    @Test
    public void mapperClass() {
        assertEquals(TestMapper.class,that.getMapperClass());
    }

    @Test
    public void reducerClass() {
        assertEquals(TestReducer.class,that.getReducerClass());
    }

    @Test
    public void beanName() {
        assertEquals("wildIsTheWind",that.getName());
    }
}

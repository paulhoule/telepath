package com.ontology2.bakemono.mapreduce;

import com.google.common.base.Function;
import static com.google.common.collect.Iterables.*;

import com.ontology2.bakemono.joins.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.springframework.beans.factory.BeanNameAware;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class SelfAwareTool<OptionsClass> extends SingleJobTool<OptionsClass> implements BeanNameAware {

    String beanName;
    static final Function<String,Path> STRING2PATH=new Function<String,Path>() {
        @Nullable @Override
        public Path apply(@Nullable String input) {
            return new Path(input);
        }
    };;

    //
    // Note that this doesn't scan interfaces,  so you can get the type of an ArrayList<X>
    // but not the type of a List<X>
    //

    public static Type[] sniffTypeParameters(Type that,Class targetClass) {
        if(that==Object.class)
            return null;

        if(that instanceof ParameterizedType) {
            ParameterizedType type=(ParameterizedType) that;
            if (type.getRawType()==targetClass)
                return type.getActualTypeArguments();
        }

        if(that==targetClass)
            throw new NoGenericTypeInformationAvailable("I can't read the generic type parameter for ["+targetClass+"] unless you subclass it with a concrete class.");

        if(that instanceof Class) {
            Class type=(Class) that;
            Type uber=type.getGenericSuperclass();
            return sniffTypeParameters(uber,targetClass);
        }

        return null;
    }

    public static <T> T readField(Object that,String name) {
        try {
            Field f=that.getClass().getField(name);
            return (T) f.get(that);
        } catch(NoSuchFieldException|IllegalAccessException ex) {
            return null;
        }
    };

    //
    // Try to instantiate this class without creating a subclass and something awful will
    // happen!
    //

    public SelfAwareTool() {
    }

    @Override
    protected String getName() {
        return beanName;
    }

    @Override
    protected Class<? extends Mapper> getMapperClass()  {
        String thisClass=getClass().getName();
        if(thisClass.endsWith("Tool")) {
            thisClass=thisClass.substring(0,thisClass.length()-4);
        }

        String tryMapper=thisClass+"Mapper";
        try {
            return (Class<? extends Mapper>) Class.forName(tryMapper);
        } catch(ClassNotFoundException x) {
            return null;
        }
    }

    @Override
    protected Class<? extends Reducer> getReducerClass()  {
        String thisClass=getClass().getName();
        if(thisClass.endsWith("Tool")) {
            thisClass=thisClass.substring(0,thisClass.length()-4);
        }

        String tryMapper=thisClass+"Reducer";
        try {
            return (Class<? extends Reducer>) Class.forName(tryMapper);
        } catch(ClassNotFoundException x) {
            return super.getReducerClass();         // necessary because some jobs don't have a reducer
        }
    }

    protected Class<? extends Writable> getMapInputKeyClass() {
        Type[] parameters=sniffTypeParameters(getMapperClass(),Mapper.class);
        return (Class<? extends Writable>) parameters[0];
    }

    protected Class<? extends Writable> getMapInputValueClass() {
        Type[] parameters=sniffTypeParameters(getMapperClass(),Mapper.class);
        return (Class<? extends Writable>) parameters[1];
    }

    @Override
    protected Class<? extends Writable> getMapOutputKeyClass() {
        Type[] parameters=sniffTypeParameters(getMapperClass(),Mapper.class);
        return (Class<? extends Writable>) parameters[2];
    }

    @Override
    protected Class<? extends Writable> getMapOutputValueClass() {
        Type[] parameters=sniffTypeParameters(getMapperClass(),Mapper.class);
        return (Class<? extends Writable>) parameters[3];
    }

    @Override
    public Class<? extends Writable> getOutputKeyClass() {
        Type[] parameters=sniffTypeParameters(getReducerClass(),Reducer.class);
        return (Class<? extends Writable>) parameters[2];
    }

    @Override
    public Class<? extends Writable> getOutputValueClass() {
        Class mapperClass=getReducerClass();
        Type[] parameters=sniffTypeParameters(getReducerClass(),Reducer.class);
        return (Class<? extends Writable>) parameters[3];
    }

    @Override
    public Iterable<Path> getInputPaths() {
        Iterable<String> s=readField(options,"input");
        if(s==null)
            return null;
        return transform(s, STRING2PATH);
    }

    @Override
    public int getNumReduceTasks() {
        Integer numReduceTasks=readField(options,"reducerCount");
        return numReduceTasks==null ? 0 : numReduceTasks;
    }

    @Override
    protected Path getOutputPath() {
        String s=readField(options,"output");
        if(s==null)
            return null;
        return STRING2PATH.apply(s);
    }

    @Override
    protected Class<? extends InputFormat> getInputFormatClass() {
        Class inKey=getMapInputKeyClass();
        Class inValue=getMapInputValueClass();
        if ( inValue==Text.class) {
            if(inKey==LongWritable.class) {
                return TextInputFormat.class;
            } else if(inKey==Text.class) {
                return KeyValueTextInputFormat.class;
            }
        }

        return SequenceFileInputFormat.class;
    }

    @Override
    protected Class<? extends OutputFormat> getOutputFormatClass() {
        Class outKey=getOutputKeyClass();
        Class outValue=getOutputValueClass();
        if (outKey==Text.class) {
            if (outValue==Text.class || outValue==NullWritable.class)
                return TextOutputFormat.class;
        } else if (outValue==Text.class && outKey==NullWritable.class)
            return TextOutputFormat.class;

        return SequenceFileOutputFormat.class;
    }

    @Override
    public Class getOptionsClass() {
        return (Class) (sniffTypeParameters(getClass(),SelfAwareTool.class))[0];
    }

    @Override
    public void setBeanName(String s) {
        beanName=s;
    }

    public static class NoGenericTypeInformationAvailable extends IllegalArgumentException {
        public NoGenericTypeInformationAvailable() {
        }

        public NoGenericTypeInformationAvailable(String s) {
            super(s);
        }

        public NoGenericTypeInformationAvailable(String message, Throwable cause) {
            super(message, cause);
        }

        public NoGenericTypeInformationAvailable(Throwable cause) {
            super(cause);
        }
    }

    protected Class<? extends RawComparator> getGroupingComparatorClass() {
        Class mapInput=getMapInputKeyClass();
        if(TaggedTextItem.class.isAssignableFrom(mapInput)) {
            return TaggedTextKeyGroupComparator.class;
        }

        return super.getGroupingComparatorClass();
    }

    protected Class<? extends Partitioner> getPartitionerClass() {
        Class mapInput=getMapInputKeyClass();
        if(TaggedItem.class.isAssignableFrom(mapInput)) {
            return TaggedKeyPartitioner.class;
        }

        return super.getPartitionerClass();
    }

    protected Class<? extends RawComparator> getSortComparatorClass() {
        Class mapInput=getMapInputKeyClass();
        if(TaggedItem.class.isAssignableFrom(mapInput)) {
            return TaggedKeyGroupComparator.class;
        }

        return super.getGroupingComparatorClass();
    }
}

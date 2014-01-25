package com.ontology2.bakemono.mapreduce;

import com.google.common.reflect.TypeToken;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

public class SmartSingleJobTool<OptionsClass> extends SingleJobTool<OptionsClass> {

    TypeToken type=TypeToken.of(getClass());

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

    //
    // Try to instantiate this class without creating a subclass and something awful will
    // happen!
    //

    public SmartSingleJobTool() {
    }

    @Override
    protected String getName() {
        return null;
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

    @Override
    protected Class<? extends Writable> getMapOutputKeyClass() {
        return null;
    }

    @Override
    protected Class<? extends Writable> getMapOutputValueClass() {
        return null;
    }

    @Override
    public Class<? extends Writable> getOutputKeyClass() {
        return null;
    }

    @Override
    public Class<? extends Writable> getOutputValueClass() {
        return null;
    }

    @Override
    public Iterable<Path> getInputPaths() {
        return null;
    }

    @Override
    public int getNumReduceTasks() {
        return 0;
    }

    @Override
    protected Path getOutputPath() {
        return null;
    }

    @Override
    public Class getOptionsClass() {
        return (Class) (sniffTypeParameters(getClass(),SmartSingleJobTool.class))[0];
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
}

package com.ontology2.telepathReports;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;

import com.ontology2.centipede.shell.CommandLineApplication;
import org.apache.commons.logging.Log;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.CharMatcher.*;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Integer.parseInt;
import static java.lang.Long.*;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.*;
import static java.util.Collections.sort;
import static java.util.Comparator.*;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.commons.logging.LogFactory.getLog;

@Component("invertMonthlies")
public class InvertMonthlies extends CommandLineApplication {

    static final Log LOG= getLog(InvertMonthlies.class);
    static String $(String s) {
        return getProperty(s);
    }

    final static int MILLION=1000000;
    final ConcurrentMap<String,ConcurrentHashMultiset<String>> data= new MapMaker().concurrencyLevel(8).initialCapacity(100).makeMap();

    private ConcurrentHashMultiset<String> bigMultiSet() {
        return ConcurrentHashMultiset.create(new MapMaker().concurrencyLevel(8).initialCapacity(3 * MILLION));
    }

    @Override
    protected void _run(String[] arguments) throws Exception {
        long t0 = currentTimeMillis();
        File f = new File("/freebase/raw/");
        ListMultimap<String, File> allFiles = ArrayListMultimap.create();

        if (f.isDirectory()) {
            for (String subDir : f.list()) {
                System.out.println(subDir);
                if (subDir.matches("20\\d{2}-\\d{2}")) {
                    File subDirF = new File(f, subDir);
                    if (subDirF.isDirectory()) {
                        data.put(subDir, bigMultiSet());
                        for (String partFile : subDirF.list())
                            if (partFile.endsWith(".gz"))
                                allFiles.put(subDir, new File(subDirF, partFile));
                    }
                }
            }
        }

        ExecutorService svc = Executors.newFixedThreadPool(8);
        List<Runnable> r = newArrayList();

        for (final String month : allFiles.keySet())
            for (final File that : allFiles.get(month)) {
                r.add(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            processFile(month, that);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

        for(Runnable that:r) {
            svc.submit(that);
        }

        svc.shutdown();
        svc.awaitTermination(1, DAYS);

        // at this point we have it all...  note that for all the screwing around we did to make this
        // concurrent,  we are getting a speedup from 45s to 25s on a four core machine when we run over
        // three months.

        long t1= currentTimeMillis();
        LOG.info("Data parse time is "+(t1-t0)+" ms");
        Set<String> allUris=newHashSet();
        for(Multiset<String> x:data.values())
            allUris.addAll(x.elementSet());
        long t2= currentTimeMillis();
        LOG.info("Name set consolidated in " + (t2 - t1) + " ms");

        final File topicHits = new File("/freebase/output/topicHits/");
        if(topicHits.exists())
            topicHits.delete();

        topicHits.mkdir();

        final int Nways=8;
        final List<Map<String,float[]>> output=newArrayList();
        for(int i=0;i<Nways;i++) {
            DB db= DBMaker.newFileDB(new File(topicHits,i+".db")).closeOnJvmShutdown().compressionEnable().make();
            Map<String,float[]> map=db.getTreeMap("uriCounts");
            output.add(map);
        }
        final List<String> orderedMonths=newArrayList(data.keySet());
        sort(orderedMonths);
        final int arrayLen=orderedMonths.size();


        final long allMonth[]=new long[arrayLen];
        int j=0;
        for(String month:orderedMonths) {
            long all=0;
            final ConcurrentHashMultiset<String> thatMonth = data.get(month);
            for(String that: thatMonth.elementSet()) {
                all+= thatMonth.count(that);
            }
            allMonth[j++]=all;
            System.out.println(month+" "+all);
        }

        long t3= currentTimeMillis();
        LOG.info("Normalization factors computed in "+(t3-t2)+ " ms");

        ExecutorService svc2 = Executors.newFixedThreadPool(6);
        LOG.info("Attempting to insert "+allUris.size()+" records");
        int k=1;
        for(final List<String> p: partition(allUris, 10000)) {

            final int kk=k++;
            svc2.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        LOG.info("Inside Runnable " + kk + " with " + p.size());
                        int knt = 0;
                        for (String uri : p) {
                            float[] rotated = new float[arrayLen];
                            int i = 0;
                            for (String month : orderedMonths) {
                                rotated[i] = (1.0F * data.get(month).count(uri) / allMonth[i]);
                                i++;
                            }
                            output.get(Math.abs(uri.hashCode() % Nways)).put(uri, rotated);
                            knt++;
                        }
                    } catch(Throwable t) {
                        if (t!=null)
                            LOG.error(t);
                    }
                }

            });
        }

        svc2.shutdown();
        svc2.awaitTermination(1,DAYS);

        long t4= currentTimeMillis();
        LOG.info("Results inverted and written in "+(t4-t3)+ " ms");
        LOG.info("Endpoint memory consumption "+ getRuntime().totalMemory());

    }

    private void processFile(String month,final File that) throws IOException {
        final ConcurrentHashMultiset<String> thisMonth=data.get(month);
        CharSource compressedFile= new CharSource() {
            @Override
            public Reader openStream() throws IOException {
                return new InputStreamReader(new GZIPInputStream(new FileInputStream(that)), Charsets.UTF_8);
            }
        };

        compressedFile.readLines(new LineProcessor() {
            final  Splitter WHITESPACE= Splitter.on(BREAKING_WHITESPACE);
            final Splitter SLASH= Splitter.on("/");
            final private CharMatcher MATCH_GT= anyOf(">");

            @Override
            public boolean processLine(String s) {
                Iterator<String> parts=WHITESPACE.split(s).iterator();
                String uri=parts.next();
                int count=parseInt(parts.next());
                String key= MATCH_GT.removeFrom(getLast(SLASH.split(uri))).intern();
                thisMonth.add(key,count);
                return true;
            }

            @Override
            public Object getResult() {
                return null;
            }
        });
    }
}

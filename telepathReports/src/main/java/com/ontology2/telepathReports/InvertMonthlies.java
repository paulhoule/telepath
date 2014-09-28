package com.ontology2.telepathReports;

import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.apache.commons.logging.Log;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.CharMatcher.*;
import static com.google.common.collect.Iterables.*;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.MoreExecutors.*;
import static java.lang.Integer.parseInt;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.*;
import static java.util.Collections.sort;
import static java.util.Collections.synchronizedMap;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.commons.logging.LogFactory.getLog;

@Component("invertMonthlies")
public class InvertMonthlies extends CommandLineApplication {

    static final Log LOG= getLog(InvertMonthlies.class);
    @Autowired
    String rawMonthlies;
    @Autowired
    String topicHitsS;
    private ArrayList<String> orderedMonths;
    private List<Multiset<String>> counts;
    private List<Float> monthNorm;

    static String $(String s) {
        return getProperty(s);
    }

    final static int MILLION=1000000;

    @Override
    protected void _run(String[] arguments) throws Exception {

        File f = new File(rawMonthlies);
        final ListMultimap<String, File> allFiles = ArrayListMultimap.create();

        if (f.isDirectory()) {
            for (String subDir : f.list()) {
                System.out.println(subDir);
                if (subDir.matches("20\\d{2}-\\d{2}")) {
                    File subDirF = new File(f, subDir);
                    if (subDirF.isDirectory()) {
                        for (String partFile : subDirF.list())
                            if (partFile.endsWith(".gz"))
                                allFiles.put(subDir, new File(subDirF, partFile));
                    }
                }
            }
        }

        long t0 = currentTimeMillis();
        readCountsFromFileAndOrderByMonth(allFiles);
        long t1= currentTimeMillis();
        LOG.info("Data parse time is "+(t1-t0)+" ms");
        LOG.info("Memory Consumption is "+ usedMemory());
        System.gc();
        LOG.info("Memory Consumption after GC is "+ usedMemory());
        Set<String> allUris=newHashSet();
        for(Multiset<String> x:counts)
            allUris.addAll(x.elementSet());


        long t2= currentTimeMillis();
        LOG.info("Name set consolidated in " + (t2 - t1) + " ms");

        System.out.println(monthNorm);

        long t3= currentTimeMillis();
        LOG.info("Normalization factors computed in "+(t3-t2)+ " ms");

        final File topicHits = new File(topicHitsS);
        if(topicHits.exists())
            topicHits.delete();

        topicHits.mkdir();

        final int Nways=8;
        final List<Map<String,float[]>> output=newArrayList();
        for(int i=0;i<Nways;i++) {
            DB db= DBMaker.newFileDB(new File(topicHits, i + ".db")).closeOnJvmShutdown().compressionEnable().make();
            Map<String,float[]> map=db.getTreeMap("uriCounts");
            output.add(map);
        }

        final int arrayLen=orderedMonths.size();

        ExecutorService svc2 = newFixedThreadPool(8);
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
                                rotated[i] = (1.0F * counts.get(i).count(uri)*monthNorm.get(i));
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

    private long usedMemory() {
        return (getRuntime().totalMemory()- getRuntime().freeMemory());
    }

    private void readCountsFromFileAndOrderByMonth(final ListMultimap<String, File> allFiles) throws InterruptedException {
        ListeningExecutorService svc = listeningDecorator(newFixedThreadPool(8));
        final Map<String,Multiset<String>> monthTag= synchronizedMap(new IdentityHashMap<String,Multiset<String>>());
        final Map<Multiset<String>,Long> hitCountTag= synchronizedMap(new IdentityHashMap<Multiset<String>,Long>());

        List<ListenableFuture<Multiset<String>>> futures=newArrayList();
        for (final String month : allFiles.keySet())
           futures.add(svc.submit(new Callable<Multiset<String>>() {
               @Override
               public Multiset<String> call() {
                   try {
                       ImmutableMultiset.Builder<String> builder = ImmutableMultiset.builder();
                       long hitCount = 0;
                       for (final File that : allFiles.get(month)) {
                           try {
                               hitCount += processFile(builder, that);
                           } catch (IOException e) {
                               LOG.error("Exception terminated thread: ",e);    // hitCount will be low!
                           }
                       }
                       Multiset<String> all = builder.build();
                       LOG.info("About to assign month tag for  " + month);
                       monthTag.put(month, all);
                       hitCountTag.put(all, hitCount);
                       return all;
                   } catch(Exception ex) {
                       LOG.error(ex);
                       return HashMultiset.create();
                   }
               }
           }));


        svc.shutdown();
        svc.awaitTermination(1, DAYS);
        orderedMonths=newArrayList(monthTag.keySet());
        sort(orderedMonths);

        counts = newArrayListWithCapacity(orderedMonths.size());
        monthNorm= newArrayListWithCapacity(orderedMonths.size());
        for(String that:orderedMonths) {
            final Multiset<String> month = monthTag.get(that);
            counts.add(month);
            final Long HIT_COUNT = hitCountTag.get(month);
            monthNorm.add(1.0F / HIT_COUNT);
        }
    };

    private long processFile(final ImmutableMultiset.Builder<String> builder, final File that) throws IOException {
        LOG.info("Processing file " + that);

        int i = 0;
        CharSource compressedFile = new CharSource() {
            @Override
            public Reader openStream() throws IOException {
                return new InputStreamReader(new GZIPInputStream(new FileInputStream(that)), Charsets.UTF_8);
            }
        };

        return compressedFile.readLines(new LineProcessor<Long>() {
            final Splitter WHITESPACE = Splitter.on(BREAKING_WHITESPACE);
            final Splitter SLASH = Splitter.on("/");
            final private CharMatcher MATCH_GT = anyOf(">");

            long hitCount = 0;

            @Override
            public boolean processLine(String s) {
                Iterator<String> parts = WHITESPACE.split(s).iterator();
                String uri = parts.next();
                try {
                    int count = parseInt(parts.next());
                    String key = MATCH_GT.removeFrom(getLast(SLASH.split(uri))).intern();
                    builder.addCopies(key, count);
                    hitCount += count;
                } catch(NumberFormatException ex) {
                    LOG.warn("Oversize integer in "+s);
                }
                return true;
            }

            @Override
            public Long getResult() {
                return hitCount;
            }
        });
    };
}

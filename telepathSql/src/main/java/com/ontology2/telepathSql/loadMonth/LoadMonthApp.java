package com.ontology2.telepathSql.loadMonth;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

@Component("loadMonth")
public class LoadMonthApp  extends CommandLineApplication {
    @Autowired DataSource telepathDataSource;


    Splitter WHITESPACE_SPLITTER= Splitter.on(CharMatcher.BREAKING_WHITESPACE);
    Log $log= LogFactory.getLog(LoadMonthApp.class);

    @Override
    protected void _run(String[] arguments) throws Exception {
        JdbcTemplate jdbcTemplate=new JdbcTemplate(telepathDataSource);

        int yrmo=yrmoToCode(2013, 1);

        for(String file:arguments) {
            $log.info("INFO: start reading file [" + file + "]");
            InputStream is=new GZIPInputStream(new FileInputStream(file));
            BufferedReader br=new BufferedReader(new InputStreamReader(is));
            while(true) {
                String line=br.readLine();
                if (line==null)
                    break;
                Iterator<String> parts=WHITESPACE_SPLITTER.split(line).iterator();

                try {
                    String lang=parts.next();
                    String uri=parts.next();
                    int count=Integer.parseInt(parts.next());
                    jdbcTemplate.update("insert into monthly (epoch,lang,uri,cnt) values (?,?,?,?)",
                            yrmo,lang,uri,count);
                } catch(NoSuchElementException|NumberFormatException ex) {
                    $log.error("Couldn't parse line ["+line+"] got exception ",ex);
                }
            }
            $log.info("INFO: stop reading file ["+file+"]");


        }
    }

    protected int yrmoToCode(int year,int month) {
        return (year-2007)*12+month;
    }
}

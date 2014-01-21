package com.ontology2.telepathSql;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Version {
    final static Log log=LogFactory.getLog(Version.class);
    final static Version instance=new Version();

    String value;
    private Version() {
        try {
            InputStream in=getClass().getResourceAsStream("version.properties");
            Properties p= new Properties();
            p.load(in);
            value=p.getProperty("com.ontology2.telepathSql.version");
            if(value==null)
                log.error("version properties file did not contain a com.ontology2.telepathSql.version property");
        } catch(IOException ioe) {
            log.error("IO Exception while reading version number",ioe);
            value=null;
        }
    }

    public static String get() {
        return instance.value;
    }
}
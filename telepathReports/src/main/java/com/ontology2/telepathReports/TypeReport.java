package com.ontology2.telepathReports;

import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component("typeReport")
public class TypeReport extends CommandLineApplication {
    @Autowired
    ApplicationContext context;

    @Override
    protected void _run(String[] strings) throws Exception {
        AutowireCapableBeanFactory f=context.getAutowireCapableBeanFactory();

        String guid = UUID.randomUUID().toString();
        TypeReportBase usage=new UsageBasedReport(guid);
        TypeReportBase links=new LinkBasedReport(guid);

        f.autowireBean(usage);
        f.autowireBean(links);

        usage._run(strings);
        links._run(strings);
    }
}

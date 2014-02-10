package com.ontology2.telepathReports;

import freemarker.template.*;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

@Component("templateService")
public class TemplateService {
    Configuration c;

    {
        c=new Configuration();
        c.setClassForTemplateLoading(SparqlService.class,"/com/ontology2/telepathReports");
        c.setObjectWrapper(new DefaultObjectWrapper());
        c.setDefaultEncoding("UTF-8");
        c.setTemplateExceptionHandler(TemplateExceptionHandler.HTML_DEBUG_HANDLER);
        c.setIncompatibleImprovements(new Version(2,3,20));
    }

    public void template(String templateId,Map<String,Object> model,Writer w) throws IOException, TemplateException {
        c.getTemplate("typeReport.ftl").process(model,w);
    }
}

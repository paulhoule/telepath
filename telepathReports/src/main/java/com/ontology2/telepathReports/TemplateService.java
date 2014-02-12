package com.ontology2.telepathReports;

import freemarker.template.*;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
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
        StringWriter innerWriter=new StringWriter();
        Template innerTemplate=c.getTemplate(templateId);
        innerTemplate.process(model, innerWriter);
        model.put("body",innerWriter.toString());

        c.getTemplate("masterTemplate.ftl").process(model,w);
    }
}

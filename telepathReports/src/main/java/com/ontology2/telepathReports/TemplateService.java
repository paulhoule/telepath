package com.ontology2.telepathReports;

import freemarker.template.*;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

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
        final String innner = subTemplate(templateId, model);
        model.put("body", subTemplate(templateId, model));
        c.getTemplate("masterTemplate.ftl").process(model,w);
    }

    //
    // run template and generate String in order to insert inside another template
    //
    public String subTemplate(String templateId, Map<String, Object> model) throws IOException, TemplateException {
        StringWriter innerWriter=new StringWriter();
        Template innerTemplate=c.getTemplate(templateId);
        innerTemplate.process(model, innerWriter);
        return innerWriter.toString();
    }

    public String subTemplate(String templateId) throws IOException, TemplateException {
        return subTemplate(templateId,new HashMap<String,Object>());
    }


}

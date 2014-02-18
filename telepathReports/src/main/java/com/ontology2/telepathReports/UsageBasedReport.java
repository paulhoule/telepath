package com.ontology2.telepathReports;

public class UsageBasedReport extends TypeReportBase {
    public UsageBasedReport(String guid) {
        super(guid);
    }

    @Override
    protected String getVariant() {
        return "usageBased";
    }

    @Override
    protected String getQueryName() {
        return "/com/ontology2/telepathReports/typeReport.sparql";
    }

    @Override
    protected String getInstanceQueryName() {
        return "/com/ontology2/telepathReports/topInstances.sparql";
    }

    @Override
    protected String getScoreExplanation() {
        return "subjectiveEye.ftl";
    }

    @Override
    protected String otherBase() {
        return "../linkBased";
    }

    @Override
    protected String otherName() {
        return "link-based";
    }
}

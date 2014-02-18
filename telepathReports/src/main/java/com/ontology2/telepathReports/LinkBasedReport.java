package com.ontology2.telepathReports;

public class LinkBasedReport extends TypeReportBase {
    public LinkBasedReport(String guid) {
        super(guid);
    }

    @Override
    protected String getVariant() {
        return "linkBased";
    }

    @Override
    protected String getQueryName() {
        return "/com/ontology2/telepathReports/typeReport-pr.sparql";
    }

    @Override
    protected String getInstanceQueryName() {
        return "/com/ontology2/telepathReports/topInstances-pr.sparql";
    }

    @Override
    protected String getScoreExplanation() {
        return "pageRank.ftl";
    }

    @Override
    protected String otherBase() {
        return "../usageBased";
    }

    @Override
    protected String otherName() {
        return "usage-based";
    }
}

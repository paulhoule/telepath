package com.ontology2;
import com.ontology2.centipede.shell.CommandLineApplication;
import org.springframework.stereotype.Component;

@Component("showVersionNumber")
public class ShowVersionNumber extends CommandLineApplication {
    @Override
    protected void _run(String[] arguments) {
        System.out.println(com.ontology2.Version.get());
    }
}
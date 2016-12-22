package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {
	static Logger logger = LogManager.getLogger("Application");

	public static void main(String[] args) {
		if(args== null || args.length==0) {
			SpringApplication.run(Application.class, args);	
		} else {
			MappingPediaRunner.run(args);
		}

	}
}

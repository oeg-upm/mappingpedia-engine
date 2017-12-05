package es.upm.fi.dia.oeg.mappingpedia;

import java.io.IOException;
import java.io.InputStream;

//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import es.upm.fi.dia.oeg.mappingpedia.utility.*;
import org.apache.jena.ontology.OntModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {


	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger("Application");
		logger.info("Working Directory = " + System.getProperty("user.dir"));
		logger.info("Starting MappingPedia Engine version 0.9.1 ...");

		InputStream is = null;
		String configurationFilename = "config.properties";
		try {

			logger.info("Loading configuration file ...");
			//String filename="config.properties";
			is = Application.class.getClassLoader().getResourceAsStream(configurationFilename);
			if(is==null){
				logger.error("Sorry, unable to find " + configurationFilename);
				return;
			}
			MappingPediaProperties properties = new MappingPediaProperties(is);
			properties.load(is);
			logger.info("Configuration file loaded.");
			MappingPediaEngine.setProperties(properties);

			GitHubUtility githubClient = new GitHubUtility(properties.githubRepository(), properties.githubUser()
					, properties.githubAccessToken()
			);
			MappingPediaEngine.githubClient_$eq(githubClient);

			CKANUtility ckanClient = new CKANUtility(properties.ckanURL(), properties.ckanKey());
			MappingPediaEngine.ckanClient_$eq(ckanClient);

			if(properties.virtuosoEnabled()) {
				VirtuosoClient virtuosoClient = new VirtuosoClient(properties.virtuosoJDBC(), properties.virtuosoUser()
						, properties.virtuosoPwd(), properties.graphName()
				);
				MappingPediaEngine.virtuosoClient_$eq(virtuosoClient);

				OntModel schemaOntology = JenaClient.loadSchemaOrgOntology(
						MappingPediaConstant.SCHEMA_ORG_FILE(), MappingPediaConstant.FORMAT());
				MappingPediaEngine.setOntologyModel(schemaOntology);
				JenaClient jenaClient = new JenaClient(schemaOntology);
				MappingPediaEngine.jenaClient_$eq(jenaClient);

			}



		} catch (Exception ex) {
			ex.printStackTrace();
		} finally{
			if(is!=null){
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		SpringApplication.run(Application.class, args);
		logger.info("Mappingpedia engine started.");
	}
}

package es.upm.fi.dia.oeg.mappingpedia;

import java.io.IOException;
import java.io.InputStream;

//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
import es.upm.fi.dia.oeg.mappingpedia.utility.CKANUtility;
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {
	public Application() {

	}

	static String configurationFilename = "config.properties";

	static Logger logger = LoggerFactory.getLogger("Application");
	static MappingPediaEngine mappingpediaEngine = null;

	public static void main(String[] args) {
		logger.info("Working Directory = " + System.getProperty("user.dir"));
		logger.info("Starting MappingPedia Engine version 1.8.1 ...");

		InputStream is = null;
		try {
			Application.mappingpediaEngine = new MappingPediaEngine();

			logger.info("Loading configuration file ...");
			//String filename="config.properties";
			is = Application.class.getClassLoader().getResourceAsStream(Application.configurationFilename);
			if(is==null){
				logger.error("Sorry, unable to find " + Application.configurationFilename);
				return;
			}
			MappingPediaProperties properties = new MappingPediaProperties(is);
			properties.load(is);
			logger.info("Configuration file loaded.");
			Application.mappingpediaEngine.mappingpediaProperties_$eq(properties);

			GitHubUtility githubClient = new GitHubUtility(properties.githubRepository(), properties.githubUser()
					, properties.githubAccessToken()
			);
			Application.mappingpediaEngine.githubClient_$eq(githubClient);
			CKANUtility ckanClient = new CKANUtility(properties.ckanURL(), properties.ckanKey());
			Application.mappingpediaEngine.ckanClient_$eq(ckanClient);



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

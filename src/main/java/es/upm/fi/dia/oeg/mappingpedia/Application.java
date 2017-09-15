package es.upm.fi.dia.oeg.mappingpedia;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.UUID;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import es.upm.fi.dia.oeg.mappingpedia.model.ListResult;
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {
	static Logger logger = LogManager.getLogger("Application");
	static MappingPediaEngine mappingpediaEngine = null;

	//static VirtGraph mappingpediaGraph = null;
	//static MappingPediaProperties prop = null;
	
	public static void main(String[] args) {
		logger.info("Working Directory = " + System.getProperty("user.dir"));

		logger.info("Starting MappingPedia Engine version 1.8.1 ...");
		//System.out.println("Running MappingPedia Engine version 1.8.1 ...");

		//Application.prop = new MappingPediaProperties();
		InputStream is = null;
		String virtuosoJDBC=null, virtuosoUser=null, virtuosoPwd=null, graphName=null, clearGraph=null;
		String githubUser=null, githubAccessToken=null, githubRepo=null, githubRepoContents=null;
		String manifestFilePath=null, mappingFilePath=null;
		String replaceMappingBaseURI=null;

		try {
			String filename="config.properties";
			is = Application.class.getClassLoader().getResourceAsStream(filename);
			if(is==null){
				logger.error("Sorry, unable to find " + filename);
				return;
			}

			MappingPediaProperties.load(is);

			//get the property value and print it out
			virtuosoJDBC = MappingPediaProperties.getProperty("vjdbc");
			logger.info("virtuosoJDBC = " + virtuosoJDBC);
			MappingPediaProperties.virtuosoJDBC_$eq(virtuosoJDBC);
			
			virtuosoUser = MappingPediaProperties.getProperty("usr");
			//logger.info("virtuosoUser = " + virtuosoUser);
			MappingPediaProperties.virtuosoUser_$eq(virtuosoUser);

			virtuosoPwd = MappingPediaProperties.getProperty("pwd");
			//logger.info("virtuosoPwd = " + virtuosoPwd);
			MappingPediaProperties.virtuosoPwd_$eq(virtuosoPwd);

			githubUser = MappingPediaProperties.getProperty("github.mappingpedia.username");
			logger.info("githubUser = " + githubUser);
			MappingPediaProperties.githubUser_$eq(githubUser);

			githubAccessToken = MappingPediaProperties.getProperty("github.mappingpedia.accesstoken");
			logger.info("github.mappingpedia.accesstoken = " + githubAccessToken);
			MappingPediaProperties.githubAccessToken_$eq(githubAccessToken);

			githubRepo = MappingPediaProperties.getProperty("github.mappingpedia.repository");
			if(githubRepo == null) {
				githubRepo = MappingPediaConstant.DEFAULT_GITHUB_REPOSITORY();
			}
			logger.info("github.mappingpedia.repository = " + githubRepo);
			MappingPediaProperties.githubRepo_$eq(githubRepo);

			githubRepoContents = MappingPediaProperties.getProperty("github.mappingpedia.repository.contents");
			if(githubRepoContents == null) {
				githubRepoContents = MappingPediaConstant.DEFAULT_GITHUB_REPOSITORY_CONTENTS();
			}
			logger.info("github.mappingpedia.repository.contents = " + githubRepoContents);
			MappingPediaProperties.githubRepoContents_$eq(githubRepoContents);


			graphName = MappingPediaProperties.getProperty("graphname");
			logger.info("graphName = " + graphName);
			MappingPediaProperties.graphName_$eq(graphName);

			clearGraph = MappingPediaProperties.getProperty("cleargraph");
			//logger.info("clearGraph = " + clearGraph);
			if(clearGraph == null) {
				MappingPediaProperties.clearGraph_$eq(false);
			} else {
				if("true".equalsIgnoreCase(clearGraph) || "yes".equalsIgnoreCase(clearGraph)) {
					MappingPediaProperties.clearGraph_$eq(true);
				} else {
					MappingPediaProperties.clearGraph_$eq(false);
				}
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
		
		//VirtGraph mappingpediaGraph = MappingPediaUtility.getVirtuosoGraph(
		//		    virtuosoJDBC, virtuosoUser, virtuosoPwd, graphName);
		//Application.mappingpediaR2RML = new MappingPediaR2RML(mappingpediaGraph);
		Application.mappingpediaEngine = new MappingPediaEngine();
		
		//Application.mappingpediaR2RML.clearGraph_$eq(false);


		SpringApplication.run(Application.class, args);
		logger.info("Mappingpedia engine started.");


	}
}

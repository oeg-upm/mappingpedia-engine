package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Properties;
import java.util.UUID;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import org.apache.jena.ontology.OntModel;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import virtuoso.jena.driver.VirtGraph;

@SpringBootApplication
public class Application {
	static Logger logger = LogManager.getLogger("Application");
	static MappingPediaR2RML mappingpediaR2RML = null;

	//static VirtGraph mappingpediaGraph = null;
	//static MappingPediaProperties prop = null;
	
	public static void main(String[] args) {
		System.out.println("Working Directory = " +
				System.getProperty("user.dir"));

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
		Application.mappingpediaR2RML = new MappingPediaR2RML();
		
		//Application.mappingpediaR2RML.clearGraph_$eq(false);


		if(args== null || args.length==0) {
			SpringApplication.run(Application.class, args);	
		} else {

			for(int i=0; i <args.length; i++) {
				if(args[i].equals("-manifestFilePath")) {
					manifestFilePath = args[i+1];
					logger.info("manifestFilePath = " + manifestFilePath);
				} else if(args[i].equals("-mappingFilePath")) {
					mappingFilePath = args[i+1];
					logger.info("mappingFilePath = " + mappingFilePath);
				} else if(args[i].equals("-replaceMappingBaseURI")) {
					replaceMappingBaseURI = args[i+1];
				}
			}
			
//			manifestFilePath = System.getProperty("manifestFilePath");
//			mappingFilePath = System.getProperty("mappingFilePath");
//			manifestText = System.getProperty("manifestText");
//			mappingText = System.getProperty("mappingText");
			
			//			MappingPediaRunner.run(manifestFilePath, mappingFilePath, virtuosoJDBC, virtuosoUser, virtuosoPwd
			//				      , graphName, clearGraph, manifestText, mappingText);
			String uuid = UUID.randomUUID().toString();
			String newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS() + uuid + "/";
			MappingPediaRunner.run(manifestFilePath, mappingFilePath, clearGraph
					, Application.mappingpediaR2RML, replaceMappingBaseURI, newMappingBaseURI);
			logger.info("Storing R2RML triples in GitHub.");

			String filename = mappingFilePath;
			if(filename == null) {
				filename = uuid + ".ttl";
			}
			String commitMessage = "Commit From mappingpedia-engine.Application";
			String mappingContent = MappingPediaR2RML.getMappingContent(manifestFilePath, mappingFilePath);
			String base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent);
			String mappingpediaUserName = "mappingpedia-testuser";
			HttpResponse<JsonNode> response = GitHubUtility.putEncodedContent(
					githubUser, githubAccessToken
					, mappingpediaUserName, uuid, filename
					, commitMessage, base64EncodedContent
					);
			int responseStatus = response.getStatus();
			if(HttpURLConnection.HTTP_CREATED == responseStatus) {
				String githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.info("githubMappingURL = " + githubMappingURL);
			}
		}

	}

	//MappingPediaUtility.loadSchemaOrgOntology();

}

package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.Properties;
import java.util.UUID;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
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
	static MappingPediaProperties prop = null;
	
	public static void main(String[] args) {
		System.out.println("Working Directory = " +
				System.getProperty("user.dir"));

		Application.prop = new MappingPediaProperties();
		InputStream is = null;
		String virtuosoJDBC=null, virtuosoUser=null, virtuosoPwd=null, graphName=null, clearGraph=null;
		String githubUser=null, githubAccessToken=null;
		String manifestFilePath=null, mappingFilePath=null, manifestText=null, mappingText=null;
		String replaceMappingBaseURI=null;

		try {
			String filename="config.properties";
			is = Application.class.getClassLoader().getResourceAsStream(filename);
			if(is==null){
				logger.error("Sorry, unable to find " + filename);
				return;
			}

			prop.load(is);

			//get the property value and print it out
			virtuosoJDBC = prop.getProperty("vjdbc");
			logger.info("virtuosoJDBC = " + virtuosoJDBC);
			Application.prop.virtuosoJDBC_$eq(virtuosoJDBC);
			
			virtuosoUser = prop.getProperty("usr");
			//logger.info("virtuosoUser = " + virtuosoUser);
			Application.prop.virtuosoUser_$eq(virtuosoUser);

			virtuosoPwd = prop.getProperty("pwd");
			//logger.info("virtuosoPwd = " + virtuosoPwd);
			Application.prop.virtuosoPwd_$eq(virtuosoPwd);

			githubUser = prop.getProperty("github.mappingpedia.username");
			logger.info("githubUser = " + githubUser);
			Application.prop.githubUser_$eq(githubUser);

			githubAccessToken = prop.getProperty("github.mappingpedia.accesstoken");
			logger.info("github.mappingpedia.accesstoken = " + githubAccessToken);
			Application.prop.githubAccessToken_$eq(githubAccessToken);

			graphName = prop.getProperty("graphname");
			logger.info("graphName = " + graphName);
			Application.prop.graphName_$eq(graphName);

			clearGraph = prop.getProperty("cleargraph");
			//logger.info("clearGraph = " + clearGraph);
			if(clearGraph == null) {
				Application.prop.clearGraph_$eq(false);
			} else {
				if("true".equalsIgnoreCase(clearGraph) || "yes".equalsIgnoreCase(clearGraph)) {
					Application.prop.clearGraph_$eq(true);
				} else {
					Application.prop.clearGraph_$eq(false);
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
		
		VirtGraph mappingpediaGraph = MappingPediaUtility.getVirtuosoGraph(
				    virtuosoJDBC, virtuosoUser, virtuosoPwd, graphName);
		Application.mappingpediaR2RML = new MappingPediaR2RML(mappingpediaGraph);
		
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
				} else if(args[i].equals("-manifestText")) {
					manifestText = args[i+1];
				} else if(args[i].equals("-mappingText")) {
					mappingText = args[i+1];
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
			MappingPediaRunner.run(manifestFilePath, manifestText, mappingFilePath, mappingText, clearGraph
					, Application.mappingpediaR2RML, replaceMappingBaseURI, newMappingBaseURI);
			logger.info("Storing R2RML triples in GitHub.");

			String filename = mappingFilePath;
			if(filename == null) {
				filename = uuid + ".ttl";
			}
			String commitMessage = "Commit From mappingpedia-engine.Application";
			String mappingContent = MappingPediaRunner.getMappingContent(manifestFilePath, manifestText, mappingFilePath, mappingText);
			String base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent);
			String mappingpediaUserName = "mappingpedia-testuser";
			HttpResponse<JsonNode> response = GitHubUtility.putEncodedFile(uuid, filename, commitMessage, base64EncodedContent
					, githubUser, githubAccessToken, mappingpediaUserName);
			int responseStatus = response.getStatus();
			if(HttpURLConnection.HTTP_CREATED == responseStatus) {
				String githubMappingURL = response.getBody().getObject().getJSONObject("content").getString("url");
				logger.info("githubMappingURL = " + githubMappingURL);
			}
		}

	}
}

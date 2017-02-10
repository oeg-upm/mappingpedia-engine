package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import es.upm.fi.dia.oeg.mappingpedia.storage.StorageService;
import virtuoso.jena.driver.VirtGraph;

@SpringBootApplication
public class Application {
	static Logger logger = LogManager.getLogger("Application");
	static MappingPediaR2RML mappingpediaR2RML = null;	
	//static VirtGraph mappingpediaGraph = null;

	public static void main(String[] args) {
		Properties prop = new Properties();
		InputStream is = null;
		String virtuosoJDBC=null, virtuosoUser=null, virtuosoPwd=null, graphName=null, clearGraph=null;
		String manifestFilePath=null, mappingFilePath=null, manifestText=null, mappingText=null;

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
			virtuosoUser = prop.getProperty("usr");
			//logger.info("virtuosoUser = " + virtuosoUser);
			virtuosoPwd = prop.getProperty("pwd");
			//logger.info("virtuosoPwd = " + virtuosoPwd);
			graphName = prop.getProperty("graphname");
			logger.info("graphName = " + graphName);
			clearGraph = prop.getProperty("cleargraph");
			//logger.info("clearGraph = " + clearGraph);
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
				} 
			}
			
//			manifestFilePath = System.getProperty("manifestFilePath");
//			mappingFilePath = System.getProperty("mappingFilePath");
//			manifestText = System.getProperty("manifestText");
//			mappingText = System.getProperty("mappingText");
			
			//			MappingPediaRunner.run(manifestFilePath, mappingFilePath, virtuosoJDBC, virtuosoUser, virtuosoPwd
			//				      , graphName, clearGraph, manifestText, mappingText);
			MappingPediaRunner.run(manifestFilePath, manifestText, mappingFilePath, mappingText, clearGraph, Application.mappingpediaR2RML);
		}

	}
}

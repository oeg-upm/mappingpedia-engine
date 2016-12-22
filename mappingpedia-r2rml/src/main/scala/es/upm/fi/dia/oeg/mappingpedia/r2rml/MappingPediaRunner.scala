package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.jena.graph.Triple;
import virtuoso.jena.driver.VirtGraph;
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.io.Source._

object MappingPediaRunner {
  val logger : Logger = LogManager.getLogger("Runner");

  def run(args: Array[String]): Unit = {
    var manifestFilePath:String = null;
    var mappingFilePath:String = null;
    var virtuosoJDBC : String = null;
    var virtuosoUser : String = null;
    var virtuosoPwd : String = null;
    var graphName : String = null;
    var clearGraphString : String = null;
    var manifestText:Option[String] = None;
    var mappingText:Option[String] = None;
    
    for(i <- 0 to args.length - 1 ) {
      if(args(i).equals("-manifestFilePath")) {
        manifestFilePath = args(i+1);
        logger.info("manifestFilePath = " + manifestFilePath);
      } else if(args(i).equals("-mappingFilePath")) {
        mappingFilePath = args(i+1);
        logger.info("mappingFilePath = " + mappingFilePath);
      } else if(args(i).equals("-vjdbc")) {
        virtuosoJDBC = args(i+1);
        logger.info("virtuosoJDBC = " + virtuosoJDBC);
      } else if(args(i).equals("-usr")) {
        virtuosoUser = args(i+1);
      } else if(args(i).equals("-pwd")) {
  		  virtuosoPwd = args(i+1);
      } else if(args(i).equals("-graphname")) {
        graphName = args(i+1);
      } else if(args(i).equals("-cleargraph")) {
        clearGraphString = args(i+1);
        logger.info("clearGraphString = " + clearGraphString);
      } else if(args(i).equals("-manifestText")) {
        manifestText = Some(args(i+1));
      } else if(args(i).equals("-mappingText")) {
        mappingText = Some(args(i+1));
      } 
    }
    
    if(graphName == null) {
      graphName = MappingPediaConstant.DEFAULT_MAPPINGPEDIA_GRAPH;
    }
    logger.info("graphName = " + graphName);

    val clearGraphBoolean = if(clearGraphString != null) {
      if(clearGraphString.equalsIgnoreCase("true") || clearGraphString.equalsIgnoreCase("yes")) {
        true;
      } else {
        false
      }
    } else {
      false
    }
    logger.info("clearGraphBoolean = " + clearGraphBoolean);
  
    val mappingpediaR2RML : MappingPediaR2RML = new MappingPediaR2RML(virtuosoJDBC, virtuosoUser
      , virtuosoPwd, graphName);


    if(manifestText == None) {
      if(manifestFilePath == null) {
        val errorMessage = "no manifest is provided";
        logger.error(errorMessage);
        throw new Exception(errorMessage);
      } else {
        val manifestFileContent = fromFile(manifestFilePath).getLines.mkString("\n");
        //logger.info("manifestFileContent = \n" + manifestFileContent);
        manifestText = Some(manifestFileContent);          
      }
    }
    
    if(mappingText == None) {
      if(mappingFilePath == null) {
        mappingFilePath = mappingpediaR2RML.getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath);
      } 
      val mappingFileContent = fromFile(mappingFilePath).getLines.mkString("\n");
      //logger.info("mappingFileContent = \n" + mappingFileContent);
      mappingText = Some(mappingFileContent);
    }
    
    mappingpediaR2RML.readManifestAndMappingInString(manifestText.get, mappingText.get);

    val virtuosoGraph = MappingPediaUtility.getVirtuosoGraph(virtuosoJDBC, virtuosoUser, virtuosoPwd
    		, graphName);
    if(clearGraphBoolean) {
      try {
        virtuosoGraph.clear();  
      } catch {
        case e:Exception => {
          logger.error("unable to clear the graph: " + e.getMessage);
        } 
      }
    	
    }
    
    logger.info("Storing manifest triples.");
    val manifestTriples = mappingpediaR2RML.getManifestTriples;
    MappingPediaUtility.store(manifestTriples, virtuosoGraph);
    
    logger.info("Storing R2RML triples.");
    val r2rmlTriples = mappingpediaR2RML.getR2rmlTriples;
    MappingPediaUtility.store(r2rmlTriples, virtuosoGraph);
    
    logger.info("Bye!");
  }




}

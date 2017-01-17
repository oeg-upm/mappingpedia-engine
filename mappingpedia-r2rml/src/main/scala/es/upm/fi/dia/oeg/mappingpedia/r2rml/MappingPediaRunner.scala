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
  val logger : Logger = LogManager.getLogger("MappingPediaRunner");

  def run(manifestFilePath:String, pManifestText:String, pMappingFilePath:String, pMappingText:String, clearGraphString:String): Unit = {
    
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
  


    val manifestText:String = if(pManifestText == null) {
      if(manifestFilePath == null) {
        val errorMessage = "no manifest is provided";
        logger.error(errorMessage);
        throw new Exception(errorMessage);
      } else {
        val manifestFileContent = fromFile(manifestFilePath).getLines.mkString("\n");
        //logger.info("manifestFileContent = \n" + manifestFileContent);
        manifestFileContent;          
      }
    } else {
      pManifestText;
    }
    
    //val mappingpediaR2RML : MappingPediaR2RML = new MappingPediaR2RML(virtuosoJDBC, virtuosoUser, virtuosoPwd, graphName);
    val mappingpediaR2RML = Application.mappingpediaR2RML;
    val virtuosoJDBC = mappingpediaR2RML.virtuosoJDBC;
    val virtuosoUser = mappingpediaR2RML.virtuosoUser;
    val virtuosoPwd = mappingpediaR2RML.virtuosoPwd;
    val graphName = mappingpediaR2RML.graphName;
    
    val mappingText:String = if(pMappingText == null) {
      val mappingFilePath = if(pMappingFilePath == null) {
        mappingpediaR2RML.getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath);
      }  else {
        pMappingFilePath;
      }
      
      val mappingFileContent = fromFile(mappingFilePath).getLines.mkString("\n");
      //logger.info("mappingFileContent = \n" + mappingFileContent);
      mappingFileContent;
    } else {
      pMappingText;
    }
    
    mappingpediaR2RML.insertMappingInString(manifestText, mappingText);

    
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
    MappingPediaUtility.store(manifestTriples, virtuosoGraph, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);
    
    logger.info("Storing R2RML triples.");
    val r2rmlTriples = mappingpediaR2RML.getR2rmlTriples;
    MappingPediaUtility.store(r2rmlTriples, virtuosoGraph, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);
    
    logger.info("Bye!");
    
  }
  
//  def run(args: Array[String]): Unit = {
//    
//
//        
//    var manifestFilePath:String = null;
//    var mappingFilePath:String = null;
//    var virtuosoJDBC:String = null;
//    var virtuosoUser:String = null;
//    var virtuosoPwd:String = null;
//    var graphName:String = null;
//    var clearGraphString:String = null;
//    var manifestText:String = null;
//    var mappingText:String = null;
//    
//    for(i <- 0 to args.length - 1 ) {
//      if(args(i).equals("-manifestFilePath")) {
//        manifestFilePath = args(i+1);
//        logger.info("manifestFilePath = " + manifestFilePath);
//      } else if(args(i).equals("-mappingFilePath")) {
//        mappingFilePath = args(i+1);
//        logger.info("mappingFilePath = " + mappingFilePath);
//      } else if(args(i).equals("-vjdbc")) {
//        virtuosoJDBC = args(i+1);
//        logger.info("virtuosoJDBC = " + virtuosoJDBC);
//      } else if(args(i).equals("-usr")) {
//        virtuosoUser = args(i+1);
//      } else if(args(i).equals("-pwd")) {
//  		  virtuosoPwd = args(i+1);
//      } else if(args(i).equals("-graphname")) {
//        graphName = args(i+1);
//      } else if(args(i).equals("-cleargraph")) {
//        clearGraphString = args(i+1);
//        logger.info("clearGraphString = " + clearGraphString);
//      } else if(args(i).equals("-manifestText")) {
//        manifestText = args(i+1);
//      } else if(args(i).equals("-mappingText")) {
//        mappingText = args(i+1);
//      } 
//    }
//
//
//        
//    if(graphName == null) {
//      graphName = MappingPediaConstant.DEFAULT_MAPPINGPEDIA_GRAPH;
//    }
//    logger.info("graphName = " + graphName);
//
//
//    this.run(manifestFilePath, mappingFilePath, virtuosoJDBC, virtuosoUser, virtuosoPwd, graphName, clearGraphString
//        , manifestText, mappingText)
//  }




}

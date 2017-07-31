package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import org.apache.jena.graph.Triple
import virtuoso.jena.driver.VirtGraph
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

import es.upm.fi.dia.oeg.mappingpedia.r2rml
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source._

object MappingPediaRunner {
 // val logger : Logger = LogManager.getLogger("MappingPediaRunner");
  val logger : Logger = LoggerFactory.getLogger(this.getClass);

  def run(manifestFilePath:String, pMappingFilePath:String, clearGraphString:String
      , mappingpediaR2RML:MappingPediaR2RML, pReplaceMappingBaseURI:String, newMappingBaseURI:String
      ): Unit = {
    
    val clearGraphBoolean = MappingPediaUtility.stringToBoolean(clearGraphString);
    logger.info("clearGraphBoolean = " + clearGraphBoolean);

    val replaceMappingBaseURI = MappingPediaUtility.stringToBoolean(pReplaceMappingBaseURI);

    val manifestText = if(manifestFilePath != null ) {
      MappingPediaR2RML.getManifestContent(manifestFilePath);
    } else {
      null;
    }

    val manifestModel = if(manifestText != null) {
      MappingPediaUtility.readModelFromString(manifestText, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);
    } else {
      null;
    }
    
    mappingpediaR2RML.manifestModel = manifestModel;

    val oldMappingText:String = MappingPediaR2RML.getMappingContent(manifestFilePath, pMappingFilePath);


    val mappingText = if(replaceMappingBaseURI) {
      MappingPediaUtility.replaceBaseURI(oldMappingText.split("\n").toIterator
        , newMappingBaseURI).mkString("\n");
    } else {
      oldMappingText;
    }
    val mappingDocumentModel = MappingPediaUtility.readModelFromString(mappingText
      , MappingPediaConstant.R2RML_FILE_LANGUAGE);
    mappingpediaR2RML.mappingDocumentModel = mappingDocumentModel;
    
    //val virtuosoGraph = mappingpediaR2RML.getMappingpediaGraph();
    val virtuosoGraph = MappingPediaUtility.getVirtuosoGraph(MappingPediaProperties.virtuosoJDBC
      , MappingPediaProperties.virtuosoUser, MappingPediaProperties.virtuosoPwd, MappingPediaProperties.graphName);
    if(clearGraphBoolean) {
      try {
        virtuosoGraph.clear();  
      } catch {
        case e:Exception => {
          logger.error("unable to clear the graph: " + e.getMessage);
        } 
      }
    }
    
    if(manifestModel != null) {
      logger.info("Storing manifest triples.");
      val manifestTriples = MappingPediaUtility.toTriples(manifestModel);
      //logger.info("manifestTriples = " + manifestTriples.mkString("\n"));
      MappingPediaUtility.store(manifestTriples, virtuosoGraph, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);
  
      logger.info("Storing generated triples.");
      val additionalTriples = mappingpediaR2RML.generateAdditionalTriples();
      logger.info("additionalTriples = " + additionalTriples.mkString("\n"));

      MappingPediaUtility.store(additionalTriples, virtuosoGraph, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);
    }
    
    logger.info("Storing R2RML triples in Virtuoso.");
    val r2rmlTriples = MappingPediaUtility.toTriples(mappingDocumentModel);
    //logger.info("r2rmlTriples = " + r2rmlTriples.mkString("\n"));

    MappingPediaUtility.store(r2rmlTriples, virtuosoGraph, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);

    logger.info("Bye!");
    
  }



}

package es.upm.fi.dia.oeg.mappingpedia.connector

import java.io.File
import java.net.URL
import java.nio.file.{Files, Path, Paths}

import be.ugent.mmlab.rml.config.RMLConfiguration
import be.ugent.mmlab.rml.core.{StdMetadataRMLEngine, StdRMLEngine}
import be.ugent.mmlab.rml.mapdochandler.extraction.std.StdRMLMappingFactory
import be.ugent.mmlab.rml.mapdochandler.retrieval.RMLDocRetrieval
//import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.{retrieveParameters}
import org.apache.commons.cli.CommandLine
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.log4j.BasicConfigurator
import org.openrdf.rio.RDFFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object RMLMapperConnector {

/*  def main(args:Array[String]):Unit = {
    val mappingURL = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/rmlio/ade242be-a58d-41bd-9c25-ec72d6b39bf5/example2.rml.ttl";
    val outputFilepath = "output.txt";
    val datasetDistributionURL = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/rmlio/ade242be-a58d-41bd-9c25-ec72d6b39bf5/example1.xml";

    val rmlMapperConnector = new RMLMapperConnector();
    rmlMapperConnector.executeWithMain(datasetDistributionURL, mappingURL, outputFilepath)

//    val args:Array[String] = Array("-m", mappingURL, "-o", outputFilepath);
//    be.ugent.mmlab.rml.main.Main.main(args);

  }*/

  /**
    *
    * @param commandLine
    * @return
    *         code taken from https://github.com/RMLio/RML-Processor/blob/ab26dac414692b3235164b271b376304869225ca/src/main/java/be/ugent/mmlab/rml/main/Main.java
    */
  def retrieveParameters(commandLine:CommandLine): Map[String, String] = {
    val parameters:Map[String, String] = Map.empty;
    var parameterKeyValue:Array[String] = null
    val parameter:String = commandLine.getOptionValue("p", null)
    val subParameters:Array[String] = parameter.split(",")
    for (subParameter <- subParameters) {
      parameterKeyValue = subParameter.split("=")
      val key = parameterKeyValue(0)
      val value = parameterKeyValue(1)
      parameters.put(key, value)
    }
    parameters
  }

}


class RMLMapperConnector() {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def executeWithMain(datasetDistributionURL: String, mappingURL:String, outputFilepath:String) = {
    logger.info(s"datasetDistributionURL = $datasetDistributionURL");
    logger.info(s"outputFilepath = $outputFilepath");

    val url = new URL(datasetDistributionURL);
    val datasetFile = new File(FilenameUtils.getName(url.getPath()));
    datasetFile.deleteOnExit();


    FileUtils.copyURLToFile(url, datasetFile)
    logger.info("datasetFile.getAbsolutePath = " + datasetFile.getAbsolutePath)

    val args: Array[String] = Array("-m", mappingURL, "-o", outputFilepath);
    try {
      be.ugent.mmlab.rml.main.Main.main(args);
    }
    catch {
      case e:Exception => { e.printStackTrace()}
    }
    finally {
      datasetFile.delete()
    }


  }



    def execute(datasetDistributionURL: String, mappingURL:String, outputFilepath:String) = {
      val url = new URL(datasetDistributionURL);
      val datasetFile = new File(FilenameUtils.getName(url.getPath()));
      datasetFile.deleteOnExit();
      FileUtils.copyURLToFile(url, datasetFile)
      val outputFile = new File(outputFilepath);
      outputFile.deleteOnExit();

      try {
        val args:Array[String] = Array("-m", mappingURL, "-o", outputFilepath);
        //be.ugent.mmlab.rml.main.Main.main(args);

        // code taken from https://github.com/RMLio/RML-Processor/blob/ab26dac414692b3235164b271b376304869225ca/src/main/java/be/ugent/mmlab/rml/main/Main.java
        var map_doc:String = null
        var triplesMap:String = null
        var exeTriplesMap:Array[String] = null
        var parameters:Map[String, String] = null
        BasicConfigurator.configure()
        var commandLine:CommandLine = null
        val mappingFactory:StdRMLMappingFactory = new StdRMLMappingFactory

        logger.info("=================================================")
        logger.info("RML Processor")
        logger.info("=================================================")
        logger.info("")

        try {
          commandLine = RMLConfiguration.parseArguments(args)
          var outputFile:String = null
          var outputFormat:String = "turtle"
          var graphName:String = ""
          var metadataVocab:String = null
          var metadataLevel:String = "None"
          var metadataFormat:String = null
          var baseIRI:String = null
          if (commandLine.hasOption("h")) RMLConfiguration.displayHelp()
          if (commandLine.hasOption("o")) outputFile = commandLine.getOptionValue("o", null)
          if (commandLine.hasOption("g")) graphName = commandLine.getOptionValue("g", "")
          if (commandLine.hasOption("p")) parameters = RMLMapperConnector.retrieveParameters(commandLine)
          if (commandLine.hasOption("f")) outputFormat = commandLine.getOptionValue("f", null)
          if (commandLine.hasOption("b")) baseIRI = commandLine.getOptionValue("b", null)
          if (commandLine.hasOption("m")) map_doc = commandLine.getOptionValue("m", null)
          if (commandLine.hasOption("md")) metadataVocab = commandLine.getOptionValue("md", null)
          if (commandLine.hasOption("mdl")) metadataLevel = commandLine.getOptionValue("mdl", null)
          if (commandLine.hasOption("mdf")) metadataFormat = commandLine.getOptionValue("mdf", null)
          logger.info("========================================")
          logger.info("Retrieving the RML Mapping Document...")
          logger.info("========================================")
          val mapDocRetrieval = new RMLDocRetrieval
          val repository = mapDocRetrieval.getMappingDoc(map_doc, RDFFormat.TURTLE)
          if (repository == null) {
            logger.debug("Problem retrieving the RML Mapping Document")
            System.exit(1)
          }
          logger.info("========================================")
          logger.info("Extracting the RML Mapping Definitions..")
          logger.info("========================================")
          val mapping = mappingFactory.extractRMLMapping(repository)
          logger.info("========================================")
          logger.info("Executing the RML Mapping..")
          logger.info("========================================")
          logger.debug("Generation Execution plan...")
          if (commandLine.hasOption("tm")) {
            triplesMap = commandLine.getOptionValue("tm", null)
            if (triplesMap != null) exeTriplesMap = RMLConfiguration.processTriplesMap(triplesMap, map_doc, baseIRI)
          }
          if (metadataLevel == "None" && metadataFormat == null && (metadataVocab == null || !metadataVocab.contains("co"))) {
            logger.debug("Mapping without metadata...")
            val engine = new StdRMLEngine(outputFile)
            engine.run(mapping, outputFile, outputFormat, graphName, parameters, exeTriplesMap, null, null, null)
          }
          else {
            logger.debug("Mapping with metadata...")
            val engine = new StdMetadataRMLEngine(outputFile)
            engine.run(mapping, outputFile, outputFormat, graphName, parameters, exeTriplesMap, metadataLevel, metadataFormat, metadataVocab)
          }
          val path = Paths.get(outputFile)
          val lines = Files.readAllLines(path)
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            logger.error("Exception " + ex)
            //RMLConfiguration.displayHelp()
            throw new Exception("Error while executing RML mapping: " + ex);
        }
      }
      catch {
        case e:Exception => { e.printStackTrace()}
      }
      finally {
        datasetFile.delete()
        outputFile.delete();
      }


  }

}

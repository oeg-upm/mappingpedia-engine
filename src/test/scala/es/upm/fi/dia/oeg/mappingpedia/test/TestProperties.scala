package es.upm.fi.dia.oeg.mappingpedia.test
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaProperties
import java.util.Properties
import java.io.FileInputStream

import es.upm.fi.dia.oeg.morph.base.MorphProperties;

object TestProperties {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val configurationFilename = "config.properties"
  
  def main(args:Array[String]) = {
    logger.info("Loading configuration file ...")
    
    val is = new FileInputStream("src/main/resources/" + configurationFilename);
    val properties = new Properties();
    properties.load(is);
    val propertiesKeySet = properties.keySet()
    logger.info(s"propertiesKeySet = ${propertiesKeySet}")

    val mappingpediaProperties = MappingPediaProperties();
    logger.info(s"mappingpediaProperties.keySet() = ${mappingpediaProperties.keySet()}")
    
    logger.info("Bye");
  }
}
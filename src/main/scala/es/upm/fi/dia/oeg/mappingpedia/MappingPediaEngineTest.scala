package es.upm.fi.dia.oeg.mappingpedia

import es.upm.fi.dia.oeg.mappingpedia.model.result.ListResult
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._


//object MappingPediaEngineTest extends App {
object MappingPediaEngineTest {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  //Application.main(args)

  val result: ListResult = MappingPediaEngine.getInstances("Place")
  logger.info(s"result = $result")
}

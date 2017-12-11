package es.upm.fi.dia.oeg.mappingpedia.utility


import java.io.{ByteArrayInputStream, File, InputStream}

import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.util.FileManager
import org.slf4j.{Logger, LoggerFactory}
import virtuoso.jena.driver.{VirtGraph, VirtModel, VirtuosoQueryExecutionFactory}

import scala.collection.JavaConversions._

class VirtuosoClient(val virtuosoJDBC:String, val virtuosoUser:String, val virtuosoPwd:String, val virtuosoGraphName:String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  val virtGraph:VirtGraph = {
    logger.info("Connecting to Virtuoso Graph...");
    new VirtGraph (virtuosoGraphName, virtuosoJDBC, virtuosoUser, virtuosoPwd);
  }

  val databaseModel = VirtModel.openDatabaseModel(virtuosoGraphName, virtuosoJDBC, virtuosoUser, virtuosoPwd);
  //val defaultModel = VirtModel.openDefaultModel(virtuosoJDBC, virtuosoUser, virtuosoPwd)

  def createQueryExecution(queryString:String) = {
    VirtuosoQueryExecutionFactory.create(queryString, this.databaseModel)
  }

  def store(file:File) : Unit = {
    val filePath = file.getPath;
    this.store(filePath)
  }

  def store(filePath:String) : Unit = {
    val model = this.readModelFromFile(filePath);
    val triples = MappingPediaUtility.toTriples(model);

    //val prop = Application.prop;

    this.store(triples);
  }

  def store(pTriples:List[Triple]) : Unit = {
    this.store(pTriples, false, null);
  }

  def store(pTriples:List[Triple], skolemizeBlankNode:Boolean, baseURI:String) : Unit = {
    val initialGraphSize = this.virtGraph.getCount();
    logger.debug("initialGraphSize = " + initialGraphSize);

    val newTriples = if(skolemizeBlankNode) { MappingPediaUtility.skolemizeTriples(pTriples, baseURI)} else { pTriples }

    val triplesIterator = newTriples.iterator;
    while(triplesIterator.hasNext) {
      val triple = triplesIterator.next();
      this.virtGraph.add(triple);
    }

    val finalGraphSize = this.virtGraph.getCount();
    logger.debug("finalGraphSize = " + finalGraphSize);

    val addedTriplesSize = finalGraphSize - initialGraphSize;
    logger.info("No of added triples = " + addedTriplesSize);
  }

  def readModelFromFile(filePath:String) : Model = {
    this.readModelFromFile(filePath, "TURTLE");
  }

  def readModelFromString(modelText:String, lang:String) : Model = {
    val inputStream = new ByteArrayInputStream(modelText.getBytes());
    val model = this.readModelFromInputStream(inputStream, lang);
    model;
  }

  def readModelFromFile(filePath:String, lang:String) : Model = {
    val inputStream = FileManager.get().open(filePath);
    val model = this.readModelFromInputStream(inputStream, lang);
    model;
  }

  def readModelFromInputStream(inputStream:InputStream, lang:String) : Model = {
    val model = ModelFactory.createDefaultModel();

    model.read(inputStream, null, lang);
    model;
  }

}


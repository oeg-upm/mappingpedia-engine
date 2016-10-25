package es.upm.fi.dia.oeg.mappingpedia.r2rml

import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.Literal
import scala.None
import com.hp.hpl.jena.graph.Triple
import virtuoso.jena.driver.VirtGraph
import org.apache.log4j.Logger

object MappingPediaUtility {
	val logger : Logger = Logger.getLogger("MappingPediaUtility");

  def getFirstPropertyObjectValueLiteral(resource:Resource, property:Property): Literal = {
		val it = resource.listProperties(property);
		var result: Literal = null;
		if(it != null && it.hasNext()) {
			val statement = it.next();
			val objectNode = statement.getObject();
			result = objectNode.asLiteral()
		}
		return result;
  }

  def getVirtuosoGraph(virtuosoJDBC : String, virtuosoUser : String, virtuosoPwd : String
		, virtuosoGraphName : String) : VirtGraph = {
				logger.info("Connecting to Virtuoso Graph.");
				val virtGraph : VirtGraph = new VirtGraph (
						virtuosoGraphName, virtuosoJDBC, virtuosoUser, virtuosoPwd);

				return virtGraph;
  }

  def store(triples : Iterable[Triple], virtuosoGraph : VirtGraph) = {
		val initialGraphSize = virtuosoGraph.getCount();
		logger.debug("initialGraphSize = " + initialGraphSize);

		val triplesIterator = triples.iterator;
		while(triplesIterator.hasNext) {
			val triple = triplesIterator.next();
			virtuosoGraph.add(triple);
		}

		val finalGraphSize = virtuosoGraph.getCount();
		logger.debug("finalGraphSize = " + finalGraphSize);

		val addedTriplesSize = finalGraphSize - initialGraphSize; 
		logger.info("No of added triples = " + addedTriplesSize);	  
  }
}
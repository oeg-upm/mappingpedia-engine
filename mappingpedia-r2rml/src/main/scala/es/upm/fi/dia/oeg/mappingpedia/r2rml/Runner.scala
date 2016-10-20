package es.upm.fi.dia.oeg.mappingpedia.r2rml

import org.apache.log4j.Logger
import com.hp.hpl.jena.graph.Triple
import virtuoso.jena.driver.VirtGraph

object Runner {
	val logger : Logger = Logger.getLogger("Runner");

	def main(args: Array[String]): Unit = {
	  var manifestFilePath : String = null;
	  var virtuosoJDBC : String = null;
	  var virtuosoUser : String = null;
	  var virtuosoPwd : String = null;
	  var clearGraph : String = null;
	  	  
	  
	  for(i <- 0 to args.length - 1 ) {
	    if(args(i).equals("-m")) {
	      manifestFilePath = args(i+1);
	    } else if(args(i).equals("-vjdbc")) {
	      virtuosoJDBC = args(i+1);
	    } else if(args(i).equals("-usr")) {
	      virtuosoUser = args(i+1);
	    } else if(args(i).equals("-pwd")) {
	      virtuosoPwd = args(i+1);
	    } else if(args(i).equals("-cleargraph")) {
	      clearGraph = args(i+1);
	    }
	  }
	  
	  logger.info("manifestFilePath = " + manifestFilePath);
	  logger.info("virtuosoJDBC = " + virtuosoJDBC);
	  logger.info("clearGraph = " + clearGraph);
	  
	  Runner.run(manifestFilePath, virtuosoJDBC, virtuosoUser, virtuosoPwd, clearGraph);
	}
	
	def run(manifestFilePath : String, virtuosoJDBC : String
	    , virtuosoUser : String, virtuosoPwd : String
	    , clearGraphString : String) : Unit = {
		
		var clearGraphBoolean = false;
		if(clearGraphString != null) {
		  if(clearGraphString.equalsIgnoreCase("true") || clearGraphString.equalsIgnoreCase("yes")) {
		    clearGraphBoolean = true;
		  }
		}
		
		this.run(manifestFilePath, virtuosoJDBC, virtuosoUser, virtuosoPwd, clearGraphBoolean);
	}
		

		  
	def run(manifestFilePath : String, virtuosoJDBC : String
	    , virtuosoUser : String, virtuosoPwd : String
	    , clearGraph : Boolean) : Unit = {

	  	val mappingPediaR2rml : MappingPediaR2RML = new MappingPediaR2RML();
		mappingPediaR2rml.readManifestFile(manifestFilePath);

		val virtuosoGraphName = mappingPediaR2rml.getGraphName;
		logger.info("Graph name = " + virtuosoGraphName);
		val virtuosoGraph = this.getVirtuosoGraph(virtuosoJDBC, virtuosoUser, virtuosoPwd
		    , virtuosoGraphName);
		if(clearGraph) {
			virtuosoGraph.clear();
		}
		
		logger.info("Storing manifest triples.");
		val manifestTriples = mappingPediaR2rml.getManifestTriples;
		this.store(manifestTriples, virtuosoGraph);
		
		logger.info("Storing R2RML triples.");
		val r2rmlTriples = mappingPediaR2rml.getR2rmlTriples;
		this.store(r2rmlTriples, virtuosoGraph);
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

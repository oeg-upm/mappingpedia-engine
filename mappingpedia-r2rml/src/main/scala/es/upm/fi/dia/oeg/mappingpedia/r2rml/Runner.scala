package es.upm.fi.dia.oeg.mappingpedia.r2rml

import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.jena.graph.Triple
import virtuoso.jena.driver.VirtGraph

object Runner {
	val logger : Logger = LogManager.getLogger("Runner");

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

    val mappingpediaR2RML : MappingPediaR2RML = new MappingPediaR2RML(virtuosoJDBC, virtuosoUser, virtuosoPwd);
//    mappingpediaR2RML.storeRDFFile(manifestFilePath, Some("TURTLE"));
    
    mappingpediaR2RML.readManifestFile(manifestFilePath);

    val virtuosoGraphName = MappingPediaConstant.MAPPINGPEDIA_GRAPH;
    logger.info("Graph name = " + virtuosoGraphName);
    val virtuosoGraph = MappingPediaUtility.getVirtuosoGraph(virtuosoJDBC, virtuosoUser, virtuosoPwd
    		, virtuosoGraphName);
    if(clearGraph) {
    	virtuosoGraph.clear();
    }
    
    logger.info("Storing manifest triples.");
    val manifestTriples = mappingpediaR2RML.getManifestTriples;
    MappingPediaUtility.store(manifestTriples, virtuosoGraph);
    
    logger.info("Storing R2RML triples.");
    val r2rmlTriples = mappingpediaR2RML.getR2rmlTriples;
    MappingPediaUtility.store(r2rmlTriples, virtuosoGraph);
  }


}

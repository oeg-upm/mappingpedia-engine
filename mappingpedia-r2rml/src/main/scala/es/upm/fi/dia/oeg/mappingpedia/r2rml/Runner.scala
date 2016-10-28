package es.upm.fi.dia.oeg.mappingpedia.r2rml;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.jena.graph.Triple;
import virtuoso.jena.driver.VirtGraph;

object Runner {
  val logger : Logger = LogManager.getLogger("Runner");

  def main(args: Array[String]): Unit = {
    var manifestFilePath : String = null;
    var virtuosoJDBC : String = null;
    var virtuosoUser : String = null;
    var virtuosoPwd : String = null;
    var graphName : String = null;
    var clearGraphString : String = null;
    

    for(i <- 0 to args.length - 1 ) {
      if(args(i).equals("-m")) {
        manifestFilePath = args(i+1);
        logger.info("manifestFilePath = " + manifestFilePath);
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
        val clearGraphString = args(i+1);
        logger.info("clearGraphString = " + clearGraphString);

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
    
    
    
    
  
    Runner.run(manifestFilePath, virtuosoJDBC, virtuosoUser, virtuosoPwd, graphName, clearGraphBoolean);
  }




  def run(manifestFilePath : String, virtuosoJDBC : String
		, virtuosoUser : String, virtuosoPwd : String
		, graphName:String, clearGraph : Boolean) : Unit = {

    val mappingpediaR2RML : MappingPediaR2RML = new MappingPediaR2RML(virtuosoJDBC, virtuosoUser
        , virtuosoPwd, graphName);
//    mappingpediaR2RML.storeRDFFile(manifestFilePath, Some("TURTLE"));
    
    mappingpediaR2RML.readManifestFile(manifestFilePath);

    val virtuosoGraph = MappingPediaUtility.getVirtuosoGraph(virtuosoJDBC, virtuosoUser, virtuosoPwd
    		, graphName);
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

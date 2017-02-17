package es.upm.fi.dia.oeg.mappingpedia.r2rml

import java.nio.channels.FileChannel


import scala.None
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.Literal
import org.apache.jena.graph.Triple
import virtuoso.jena.driver.VirtGraph
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import java.io._
import org.apache.jena.util.FileManager
import org.apache.jena.graph.Node
import org.apache.jena.graph.NodeFactory
import org.eclipse.egit.github.core.client.GitHubClient
import org.eclipse.egit.github.core.service.RepositoryService
import scala.collection.JavaConversions._


object MappingPediaUtility {
	val logger : Logger = LogManager.getLogger("MappingPediaUtility");
  
/*
  def main(args: Array[String]): Unit = {
    //Basic authentication
    val client = new GitHubClient();
    client.setCredentials("user", "passw0rd");
    
    val service = new RepositoryService();
    val repositories = service.getRepositories("fpriyatna")
    for (repo <- repositories) {
      println(repo.getName() + " Watchers: " + repo.getWatchers());
    }
    

    println("Bye!")
  }
*/

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
				logger.info("Connecting to Virtuoso Graph...");
				val virtGraph : VirtGraph = new VirtGraph (
						virtuosoGraphName, virtuosoJDBC, virtuosoUser, virtuosoPwd);
        logger.info("Connected to Virtuoso Graph...");
				return virtGraph;
  }

  def store(filePath:String, graphURI:String) : Unit = {
    val model = this.readModelFromFile(filePath);
    val triples = this.toTriples(model);

    val prop = Application.prop;
    val virtuosoGraph = this.getVirtuosoGraph(prop.virtuosoJDBC, prop.virtuosoUser, prop.virtuosoPwd, graphURI);

    this.store(triples, virtuosoGraph);
  }

  def store(pTriples:List[Triple], virtuosoGraph:VirtGraph) : Unit = {
    this.store(pTriples, virtuosoGraph, false, null);
  }

  def store(pTriples:List[Triple], virtuosoGraph:VirtGraph, skolemizeBlankNode:Boolean, baseURI:String) : Unit = {
		val initialGraphSize = virtuosoGraph.getCount();
		logger.debug("initialGraphSize = " + initialGraphSize);

		val newTriples = if(skolemizeBlankNode) { this.skolemizeTriples(pTriples, baseURI)} else { pTriples }
		
		val triplesIterator = newTriples.iterator;
		while(triplesIterator.hasNext) {
			val triple = triplesIterator.next();
			virtuosoGraph.add(triple);
		}

		val finalGraphSize = virtuosoGraph.getCount();
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

  def collectBlankNodes(triples:List[Triple]) : Set[Node] = {
    val blankNodes:Set[Node] = if(triples.isEmpty) {
      Set.empty;
    } else {
      val blankNodesHead:Set[Node] = this.collectBlankNode(triples.head);
      val blankNodesTail:Set[Node] = this.collectBlankNodes(triples.tail);
    
      blankNodesHead ++ blankNodesTail;      
    }

    blankNodes;
  }
  
  def collectBlankNode(tp:Triple) : Set[Node] = {
    val tpSubject = tp.getSubject;
    val tpObject = tp.getObject;
    
    var blankNodes:Set[Node] = Set.empty;
    
    if(tpSubject.isBlank()) { 
      blankNodes = blankNodes + tpSubject;
    }
    
    if(tpObject.isBlank()) {
      blankNodes = blankNodes + tpObject;
    }
    
    blankNodes;
  }
  
  def skolemizeTriples(triples:List[Triple], baseURI:String) : List[Triple] = {
    val blankNodes = this.collectBlankNodes(triples);
    val mapNewNodes = this.skolemizeBlankNodes(blankNodes, baseURI);
    val newTriples = this.replaceBlankNodesInTriples(triples, mapNewNodes);
    newTriples;
  }
  
  def replaceBlankNodesInTriples(triples:List[Triple], mapNewNodes:Map[Node, Node]) : List[Triple] = {
    val newTriples = triples.map(x => {this.replaceBlankNodesInTriple(x, mapNewNodes)});
    newTriples;
  }
  
  def replaceBlankNodesInTriple(tp:Triple, mapNewNodes:Map[Node, Node]) : Triple = {
    val tpSubject = tp.getSubject;
    val tpObject = tp.getObject;
    
    val tpSubjectNew:Node = if(tpSubject.isBlank()) { mapNewNodes.get(tpSubject).get } else { tpSubject; }
    val tpObjectNew:Node = if(tpObject.isBlank()) { mapNewNodes.get(tpObject).get } else { tpObject; }
    
    val newTriple = new Triple(tpSubjectNew, tp.getPredicate, tpObjectNew);
    newTriple;
  }
  
  def skolemizeBlankNodes(blankNodes:Set[Node], baseURI:String) : Map[Node, Node] = {
    val mapNewNodes = blankNodes.map(x => {(x -> this.skolemizeBlankNode(x, baseURI))}).toMap;
    mapNewNodes;
  }
  
  def skolemizeBlankNode(blankNode:Node, baseURI:String) : Node = {
    //val absoluteBaseURI = if(baseURI.endsWith("/")) { baseURI } else { baseURI + "/" }
    
    val newNodeURI = baseURI + ".well-known/genid/" + blankNode.getBlankNodeLabel;
    val newNode = NodeFactory.createURI(newNodeURI);
    newNode;
  }
  
  def toTriples(model:Model) : List[Triple] = {
    val statements = model.listStatements();
    //val statementList = statements.toList();
    var triples:List[Triple] = List.empty; 
    if(statements != null) {
      while(statements.hasNext()) {
        val statement = statements.nextStatement();
        val triple = statement.asTriple();
        triples = triples ::: List(triple);
      }
    }
    triples;
  }

  def replaceBaseURI(lines:Iterator[String], pNewBaseURI:String) : Iterator[String] = {
    var newBaseURI = pNewBaseURI;
    if(!pNewBaseURI.startsWith("<")) {
      newBaseURI = "<" + newBaseURI;
    }
    if(!pNewBaseURI.endsWith(">")) {
      newBaseURI = newBaseURI + ">";
    }

    val newLines = lines.map(line =>
      if(line.startsWith("@base")) {
        "@base " + newBaseURI + " . ";
      } else { line }
    )
    newLines;
  }

  @throws(classOf[IOException])
  def materializeFileInputStream(source: FileInputStream, dest: File) {
    var sourceChannel: FileChannel = null
    var destChannel: FileChannel = null
    val fos: FileOutputStream = new FileOutputStream(dest)
    try {
      sourceChannel = source.getChannel
      destChannel = fos.getChannel
      destChannel.transferFrom(sourceChannel, 0, sourceChannel.size)
    } finally {
      sourceChannel.close
      destChannel.close
      fos.close
    }
  }

  @throws(classOf[IOException])
  def materializeFileInputStream(source: FileInputStream, uuidDirectoryName: String, fileName: String): File = {
    val uploadDirectoryPath: String = "upload-dir"
    val outputDirectory: File = new File(uploadDirectoryPath)
    if (!outputDirectory.exists) {
      outputDirectory.mkdirs
    }
    val uuidDirectoryPath: String = uploadDirectoryPath + "/" + uuidDirectoryName
    logger.info("upload directory path = " + uuidDirectoryPath)
    val uuidDirectory: File = new File(uuidDirectoryPath)
    if (!uuidDirectory.exists) {
      uuidDirectory.mkdirs
    }
    val uploadedFilePath: String = uuidDirectory + "/" + fileName
    val dest: File = new File(uploadedFilePath)
    dest.createNewFile
    this.materializeFileInputStream(source, dest)
    return dest
  }

  def pushContentToGitHub(file:File) = {

  }
}
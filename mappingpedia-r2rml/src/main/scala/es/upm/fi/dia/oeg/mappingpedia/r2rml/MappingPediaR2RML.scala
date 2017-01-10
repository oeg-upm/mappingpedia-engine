package es.upm.fi.dia.oeg.mappingpedia.r2rml

import java.io.File
import scala.collection.mutable.ListBuffer

import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.util.FileManager
import org.apache.jena.vocabulary.RDF
import org.apache.jena.vocabulary.DC_11
import org.apache.jena.rdf.model.Statement
import org.apache.jena.graph.Triple
import org.apache.jena.rdf.model.RDFList
import org.apache.jena.rdf.model.Resource
import org.apache.jena.rdf.model.Property
import org.apache.jena.rdf.model.RDFNode
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager
import virtuoso.jena.driver.VirtGraph
import org.apache.jena.graph.Node
import org.apache.jena.graph.NodeFactory
import java.nio.file.Files
import java.nio.file.Paths
import java.security.MessageDigest
import org.apache.jena.util.ResourceUtils
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.impl.StatementImpl

class MappingPediaR2RML(val virtuosoJDBC:String, val virtuosoUser:String
    , val virtuosoPwd:String, val graphName:String) {
	val logger : Logger = LogManager.getLogger("MappingPediaR2RML");
	
	private val MANIFEST_FILE_LANGUAGE = "TURTLE";
	private val R2RML_FILE_LANGUAGE = "TURTLE";
	private var r2rmlTriples : List[Triple] = Nil;
	private var manifestTriples : List[Triple] = Nil;
  val mappingpediaGraph:VirtGraph = MappingPediaUtility.getVirtuosoGraph(
    virtuosoJDBC, virtuosoUser, virtuosoPwd, graphName);
		
  def insertMappingInString(manifestText:String, mappingText:String) = {
    logger.info("reading manifest file ...");
    val manifestModel = MappingPediaUtility.readModelFromString(manifestText, MANIFEST_FILE_LANGUAGE);
    
    logger.info("reading r2rml file ...");
    val r2rmlDocumentModel = MappingPediaUtility.readModelFromString(mappingText, R2RML_FILE_LANGUAGE);
    
    this.insertMappingInModel(manifestModel, r2rmlDocumentModel);
  }
  
  def insertMappingInModel(manifestModel:Model, r2rmlDocumentModel:Model) = {
		val r2rmlMappingDocumentResources = manifestModel.listResourcesWithProperty(
				RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS);
		
		if(r2rmlMappingDocumentResources != null) {
			val r2rmlMappingDocument = r2rmlMappingDocumentResources.nextResource();
			
			val baseNS : String = r2rmlDocumentModel.getNsPrefixURI("");
			logger.info("baseNS = " + baseNS);
			
			val r2rmlMappingDocumentStatements = r2rmlDocumentModel.listStatements();
			if(r2rmlMappingDocumentStatements != null) {
			  while(r2rmlMappingDocumentStatements.hasNext()) {
			    val r2rmlMappingDocumentStatement : Statement = r2rmlMappingDocumentStatements.nextStatement();
			    val r2rmlMappingDocumentTriple = r2rmlMappingDocumentStatement.asTriple();
			    r2rmlTriples = r2rmlTriples ::: List(r2rmlMappingDocumentTriple);
			  }
			}

			val triplesMapResources = r2rmlDocumentModel.listResourcesWithProperty(
				RDF.`type`, MappingPediaConstant.R2RML_TRIPLESMAP_CLASS);
			if(triplesMapResources != null) {
			  while(triplesMapResources.hasNext()) {
			    val triplesMapResource = triplesMapResources.nextResource();
			    val newStatement = new StatementImpl(r2rmlMappingDocument, MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMapResource);
			    logger.info("adding new hasTriplesMap statement: " + newStatement);
			    manifestModel.add(newStatement);
			  }
			  
			}

		}

		//put this after we process triples Map List!
		val r2rmlMappingDocumentStatements = manifestModel.listStatements();
		if(r2rmlMappingDocumentStatements != null) {
		  while(r2rmlMappingDocumentStatements.hasNext()) {
		    val r2rmlMappingDocumentTriple = r2rmlMappingDocumentStatements.next().asTriple();
		    manifestTriples = manifestTriples ::: List(r2rmlMappingDocumentTriple);
		  }
		}
		
		//logger.debug("manifestTriples = " + manifestTriples);
		//logger.debug("r2rmlTriples = " + r2rmlTriples);
		

  }
  
  def getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath:String) : String = {
		logger.info("Reading manifest file : " + manifestFilePath);

		val manifestModel = MappingPediaUtility.readModelFromFile(manifestFilePath, null, MANIFEST_FILE_LANGUAGE);
		
		val r2rmlResources = manifestModel.listResourcesWithProperty(
				RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS);
		
		if(r2rmlResources != null) {
			val r2rmlResource = r2rmlResources.nextResource();

			val mappingDocumentFilePath = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
					r2rmlResource, MappingPediaConstant.DEFAULT_MAPPINGDOCUMENTFILE_PROPERTY).toString();

			var mappingDocumentFile = new File(mappingDocumentFilePath.toString());
			val isMappingDocumentFilePathAbsolute = mappingDocumentFile.isAbsolute();
			var r2rmlMappingDocumentPath : String = null; 
			if(isMappingDocumentFilePathAbsolute) {
				r2rmlMappingDocumentPath = mappingDocumentFilePath
			} else {
			  val manifestFile = new File(manifestFilePath);
			  if(manifestFile.isAbsolute()) {
				r2rmlMappingDocumentPath = manifestFile.getParentFile().toString() + File.separator + mappingDocumentFile; 
			  } else {
			    r2rmlMappingDocumentPath = mappingDocumentFilePath
			  }
			}
			r2rmlMappingDocumentPath
			
		} else {
        val errorMessage = "mapping file is not specified in the manifest file";
        logger.error(errorMessage);
        throw new Exception(errorMessage);
		}

  }
  
	def insertMappingFromManifestFile(manifestFilePath : String) = {
		logger.info("Reading manifest file : " + manifestFilePath);
		
		val manifestModel = MappingPediaUtility.readModelFromFile(manifestFilePath, null, MANIFEST_FILE_LANGUAGE);
		
		var r2rmlDocumentModel:Model = null;
		val r2rmlResources = manifestModel.listResourcesWithProperty(
				RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS);
		
		if(r2rmlResources != null) {
			val r2rmlResource = r2rmlResources.nextResource();

			val mappingDocumentFilePath = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
					r2rmlResource, MappingPediaConstant.DEFAULT_MAPPINGDOCUMENTFILE_PROPERTY).toString();

			var mappingDocumentFile = new File(mappingDocumentFilePath.toString());
			val isMappingDocumentFilePathAbsolute = mappingDocumentFile.isAbsolute();
			var r2rmlMappingDocumentPath : String = null; 
			if(isMappingDocumentFilePathAbsolute) {
				r2rmlMappingDocumentPath = mappingDocumentFilePath
			} else {
			  val manifestFile = new File(manifestFilePath);
			  if(manifestFile.isAbsolute()) {
				r2rmlMappingDocumentPath = manifestFile.getParentFile().toString() + File.separator + mappingDocumentFile; 
			  } else {
			    r2rmlMappingDocumentPath = mappingDocumentFilePath
			  }
			}
			
			logger.info("Reading R2RML Mapping document : " + r2rmlMappingDocumentPath);
			r2rmlDocumentModel = ModelFactory.createDefaultModel();
			val inR2rmlDocumentModel = FileManager.get().open( r2rmlMappingDocumentPath );
			r2rmlDocumentModel.read(inR2rmlDocumentModel, null, R2RML_FILE_LANGUAGE);
		}
		
		this.insertMappingInModel(manifestModel, r2rmlDocumentModel);
	}

	def getR2rmlTriples : List[Triple] = {
		return r2rmlTriples;
	}
	
	def getManifestTriples : List[Triple] = {
		return manifestTriples;
	}	
	
	def getGraphName : String = {
		return graphName;
	}
	
  
def storeRDFFile(turtleFilePath:String, rdfSyntax:Option[String]) = {

    val model = ModelFactory.createDefaultModel() ;
    if(rdfSyntax == null || rdfSyntax.isEmpty) {
      model.read(new File(turtleFilePath).toURL().toString());
    } else {
      model.read(new File(turtleFilePath).toURL().toString(), "TURTLE");  
    }
    
    logger.info("RDF file read.");
    
    val triplesMapResourcesList = model.listResourcesWithProperty(
      RDF.`type`, MappingPediaConstant.R2RML_TRIPLESMAP_CLASS);
    if(triplesMapResourcesList != null) {
      if(triplesMapResourcesList.hasNext()) {
        val triplesMapResource = triplesMapResourcesList.nextResource();
        logger.info("triplesMapResource = " + triplesMapResource);
        
        val freshBlankNode = NodeFactory.createBlankNode();
        logger.info("freshBlankNode = " + freshBlankNode);
        
        val newResource = 
          ResourceUtils.renameResource(triplesMapResource, freshBlankNode.getBlankNodeLabel);
        logger.info("newResource = " + newResource);
      }
    }
	 		
		val initialGraphSize = mappingpediaGraph.getCount();
		logger.debug("initialGraphSize = " + initialGraphSize);

		val stmtIterator = model.listStatements();
    while(stmtIterator.hasNext()) {
      val stmt = stmtIterator.nextStatement();
      logger.info("stmt = " + stmt);
      
      val triple = stmt.asTriple();
      logger.info("triple = " + triple);
      
      mappingpediaGraph.add(triple);
    }
    
    val finalGraphSize = mappingpediaGraph.getCount();
		logger.debug("finalGraphSize = " + finalGraphSize);
		val addedTriplesSize = finalGraphSize - initialGraphSize; 
		logger.info("No of added triples = " + addedTriplesSize);
    
  }
  
  
}

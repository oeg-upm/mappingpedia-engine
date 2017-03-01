package es.upm.fi.dia.oeg.mappingpedia.r2rml

import java.io.File
import java.net.HttpURLConnection
import com.mashape.unirest.http.{JsonNode, HttpResponse}
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVRunnerFactory, MorphCSVProperties}
import org.apache.commons.lang.text.StrSubstitutor

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
import scala.collection.JavaConversions._


class MappingPediaR2RML(mappingpediaGraph:VirtGraph) {
	val logger : Logger = LogManager.getLogger("MappingPediaR2RML");
	var manifestModel:Model = null;
	var mappingDocumentModel:Model = null;
	
//	private var r2rmlTriples : List[Triple] = Nil;
//	private var manifestTriples : List[Triple] = Nil;
	var clearGraph:Boolean = false;
	

		
//  def insertMappingInString(manifestText:String, mappingText:String) = {
//    logger.info("reading manifest file ...");
//    val manifestModel = MappingPediaUtility.readModelFromString(manifestText, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);
//    
//    logger.info("reading r2rml file ...");
//    val r2rmlDocumentModel = MappingPediaUtility.readModelFromString(mappingText, MappingPediaConstant.R2RML_FILE_LANGUAGE);
//    
//    this.insertTriplesMapsIntoManifest(manifestModel, r2rmlDocumentModel);
//  }
  
  def generateAdditionalTriples() : List[Triple] = {
    var newTriples:List[Triple] = List.empty;
    
    val r2rmlMappingDocumentResources = manifestModel.listResourcesWithProperty(
				RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS);
		
		if(r2rmlMappingDocumentResources != null) {
		  while(r2rmlMappingDocumentResources.hasNext()) {
  			val r2rmlMappingDocumentResource = r2rmlMappingDocumentResources.nextResource();
  
  			val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
  				RDF.`type`, MappingPediaConstant.R2RML_TRIPLESMAP_CLASS);
  			if(triplesMapResources != null) {
  			  while(triplesMapResources.hasNext()) {
  			    val triplesMapResource = triplesMapResources.nextResource();
  			    val newStatement = new StatementImpl(r2rmlMappingDocumentResource, MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMapResource);
  			    logger.info("adding new hasTriplesMap statement: " + newStatement);
  			    val newTriple = newStatement.asTriple();
  			    newTriples = newTriples ::: List(newTriple);
  			  }
  			  
  			}		    
		  }
		}

		newTriples;
  }
  
  
//  def insertMappingFromManifestFile(manifestFile:File) = {
//    
//  }
  
//	def insertMappingFromManifestFilePath(manifestFilePath : String) = {
//		logger.info("Reading manifest file : " + manifestFilePath);
//		
//		val manifestModel = MappingPediaUtility.readModelFromFile(manifestFilePath, null, MANIFEST_FILE_LANGUAGE);
//		
//		var r2rmlDocumentModel:Model = null;
//		val r2rmlResources = manifestModel.listResourcesWithProperty(
//				RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS);
//		
//		if(r2rmlResources != null) {
//			val r2rmlResource = r2rmlResources.nextResource();
//
//			val mappingDocumentFilePath = MappingPediaUtility.getFirstPropertyObjectValueLiteral(
//					r2rmlResource, MappingPediaConstant.DEFAULT_MAPPINGDOCUMENTFILE_PROPERTY).toString();
//      logger.info("mappingDocumentFilePath = " + mappingDocumentFilePath);
//
//			var mappingDocumentFile = new File(mappingDocumentFilePath.toString());
//			val isMappingDocumentFilePathAbsolute = mappingDocumentFile.isAbsolute();
//			var r2rmlMappingDocumentPath : String = null; 
//			if(isMappingDocumentFilePathAbsolute) {
//				r2rmlMappingDocumentPath = mappingDocumentFilePath
//			} else {
//			  val manifestFile = new File(manifestFilePath);
//			  if(manifestFile.isAbsolute()) {
//				r2rmlMappingDocumentPath = manifestFile.getParentFile().toString() + File.separator + mappingDocumentFile; 
//			  } else {
//			    r2rmlMappingDocumentPath = mappingDocumentFilePath
//			  }
//			}
//			
//			logger.info("Reading R2RML Mapping document : " + r2rmlMappingDocumentPath);
//			r2rmlDocumentModel = ModelFactory.createDefaultModel();
//			val inR2rmlDocumentModel = FileManager.get().open( r2rmlMappingDocumentPath );
//			r2rmlDocumentModel.read(inR2rmlDocumentModel, null, R2RML_FILE_LANGUAGE);
//		}
//		
//		this.insertTriplesMapsIntoManifest(manifestModel, r2rmlDocumentModel);
//	}

	
//	def getGraphName : String = {
//		return graphName;
//	}
	
  
//def storeRDFFile(turtleFilePath:String, rdfSyntax:Option[String]) = {
//
//    val model = ModelFactory.createDefaultModel() ;
//    if(rdfSyntax == null || rdfSyntax.isEmpty) {
//      model.read(new File(turtleFilePath).toURL().toString());
//    } else {
//      model.read(new File(turtleFilePath).toURL().toString(), "TURTLE");  
//    }
//    
//    logger.info("RDF file read.");
//    
//    val triplesMapResourcesList = model.listResourcesWithProperty(
//      RDF.`type`, MappingPediaConstant.R2RML_TRIPLESMAP_CLASS);
//    if(triplesMapResourcesList != null) {
//      if(triplesMapResourcesList.hasNext()) {
//        val triplesMapResource = triplesMapResourcesList.nextResource();
//        logger.info("triplesMapResource = " + triplesMapResource);
//        
//        val freshBlankNode = NodeFactory.createBlankNode();
//        logger.info("freshBlankNode = " + freshBlankNode);
//        
//        val newResource = 
//          ResourceUtils.renameResource(triplesMapResource, freshBlankNode.getBlankNodeLabel);
//        logger.info("newResource = " + newResource);
//      }
//    }
//	 		
//		val initialGraphSize = mappingpediaGraph.getCount();
//		logger.debug("initialGraphSize = " + initialGraphSize);
//
//		val stmtIterator = model.listStatements();
//    while(stmtIterator.hasNext()) {
//      val stmt = stmtIterator.nextStatement();
//      logger.info("stmt = " + stmt);
//      
//      val triple = stmt.asTriple();
//      logger.info("triple = " + triple);
//      
//      mappingpediaGraph.add(triple);
//    }
//    
//    val finalGraphSize = mappingpediaGraph.getCount();
//		logger.debug("finalGraphSize = " + finalGraphSize);
//		val addedTriplesSize = finalGraphSize - initialGraphSize; 
//		logger.info("No of added triples = " + addedTriplesSize);
//    
//  }

//  def getVirtuosoGraph() : VirtGraph = {
//    val virtuosoGraph = MappingPediaUtility.getVirtuosoGraph(this.virtuosoJDBC, this.virtuosoUser, this.virtuosoPwd
//    		, this.graphName);
//    virtuosoGraph;
//  }
	
  def getMappingpediaGraph() = this.mappingpediaGraph;
  
}

object MappingPediaR2RML {
  val logger : Logger = LogManager.getLogger("MappingPediaR2RML");
  
  def getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath:String) : String = {
		logger.info("Reading manifest file : " + manifestFilePath);

		val manifestModel = MappingPediaUtility.readModelFromFile(manifestFilePath, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);
		
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

	def executeMapping(mappingpediaUsername:String, mappingDirectory: String, mappingFilename: String
		, datasetFile: String, pOutputFilename: String) : MappingPediaExecutionResult = {
		logger.info("mappingpediaUsername = " + mappingpediaUsername)
		logger.info("mappingDirectory = " + mappingDirectory)
		logger.info("mappingFilename = " + mappingFilename)
		val properties: MorphCSVProperties = new MorphCSVProperties
		properties.setDatabaseName(mappingpediaUsername + "/" + mappingDirectory)
		val templateString: String = "${mappingpediaUsername}/${mappingDirectory}/${mappingFilename}"
		val valuesMap: java.util.Map[String, String] = new java.util.HashMap[String, String]
		valuesMap.put("mappingpediaUsername", mappingpediaUsername)
		valuesMap.put("mappingDirectory", mappingDirectory)
		valuesMap.put("mappingFilename", mappingFilename)
		val sub: StrSubstitutor = new StrSubstitutor(valuesMap)
		val templateResultString: String = sub.replace(templateString)
		val mappingBlobURL: String = "https://github.com/oeg-upm/mappingpedia-contents/blob/master/" + templateResultString
		logger.info("mappingBlobURL = " + mappingBlobURL)
		properties.setMappingDocumentFilePath(mappingBlobURL)
		val outputFileName = if (pOutputFilename == null) {
			"output.nt";
		} else {
			pOutputFilename;
		}
		val outputFilepath = "executions/" + templateResultString + "/" + outputFileName

		properties.setOutputFilePath(outputFilepath);
		properties.setCSVFile(datasetFile)
		try {
			val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
			val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
			runner.run
			logger.info("mapping execution success!")
			val outputFile: File = new File(outputFilepath)
			val response = GitHubUtility.putEncodedFile(Application.prop.githubUser, Application.prop.githubAccessToken
				, mappingpediaUsername, mappingDirectory, outputFileName
				, "add mapping execution result by mappingpedia engine", outputFile);

			val responseStatus: Int = response.getStatus
			logger.info("responseStatus = " + responseStatus)
			val responseStatusText: String = response.getStatusText
			logger.info("responseStatusText = " + responseStatusText)
			if (HttpURLConnection.HTTP_OK == responseStatus) {
				val outputGitHubURL: String = response.getBody.getObject.getJSONObject("content").getString("url");
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null, outputGitHubURL, responseStatus)
				return executionResult
			}
			else {
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null, responseStatusText, responseStatus)
				return executionResult
			}
		}
		catch {
			case e: Exception => {
				e.printStackTrace
				val errorMessage: String = "Error occured: " + e.getMessage
				logger.error("mapping execution failed: " + errorMessage)
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null, errorMessage, HttpURLConnection.HTTP_INTERNAL_ERROR)
				return executionResult
			}
		}
	}

}
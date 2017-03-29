package es.upm.fi.dia.oeg.mappingpedia.r2rml

import java.io.File
import java.net.HttpURLConnection

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory}
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
import java.util.UUID

import org.apache.jena.util.ResourceUtils
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.impl.StatementImpl
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.multipart.MultipartFile

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


	def addDatasetFile(datasetFileRef: MultipartFile, mappingpediaUsername:String, datasetID:String) : MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

		try {
			val datasetFile:File = MappingPediaUtility.multipartFileToFile(datasetFileRef, datasetID)

			logger.info("storing a new dataset file in github ...")
			val commitMessage = "Add a new dataset file by mappingpedia-engine"
			val response = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername
				, datasetID, datasetFile.getName, commitMessage, datasetFile)
			logger.debug("response.getHeaders = " + response.getHeaders)
			logger.debug("response.getBody = " + response.getBody)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)
			if (HttpURLConnection.HTTP_CREATED == responseStatus) {
				val datasetURL = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("datasetURL = " + datasetURL)
				logger.info("dataset stored.")
				val executionResult = new MappingPediaExecutionResult(null, datasetURL, null
					, null, null, responseStatusText, responseStatus)
				return executionResult
			}
			else {
				val executionResult = new MappingPediaExecutionResult(null, null, null
					, null , null, responseStatusText, responseStatus)
				return executionResult
			}
		} catch {
			case e: Exception =>
				val errorMessage = e.getMessage
				logger.error("error uploading a new file: " + errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new MappingPediaExecutionResult(null, null, null
					, null, null, errorMessage, errorCode)
				return executionResult
		}
	}

	def addQueryFile(queryFileRef: MultipartFile, mappingpediaUsername:String, datasetID:String) : MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

		try {
			val queryFile:File = MappingPediaUtility.multipartFileToFile(queryFileRef, datasetID)

			logger.info("storing a new query file in github ...")
			val commitMessage = "Add a new query file by mappingpedia-engine"
			val response = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername
				, datasetID, queryFile.getName, commitMessage, queryFile)
			logger.debug("response.getHeaders = " + response.getHeaders)
			logger.debug("response.getBody = " + response.getBody)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)
			if (HttpURLConnection.HTTP_CREATED == responseStatus) {
				val queryURL = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("queryURL = " + queryURL)
				logger.info("dataset stored.")
				val executionResult = new MappingPediaExecutionResult(null, null, null
					, queryURL, null, responseStatusText, responseStatus)
				return executionResult
			}
			else {
				val executionResult = new MappingPediaExecutionResult(null, null, null
					, null , null, responseStatusText, responseStatus)
				return executionResult
			}
		} catch {
			case e: Exception =>
				val errorMessage = e.getMessage
				logger.error("error uploading a new query file: " + errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new MappingPediaExecutionResult(null, null, null
					, null, null, errorMessage, errorCode)
				return executionResult
		}
	}

	def executeMapping(mappingpediaUsername:String, mappingDirectory: String, mappingFilename: String
		, datasetFile: String, queryFile:String, pOutputFilename: String) : MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		val properties: MorphCSVProperties = new MorphCSVProperties
		properties.setDatabaseName(mappingpediaUsername + "/" + mappingDirectory)
		val templateString: String = "${mappingpediaUsername}/${mappingDirectory}/${mappingFilename}"
		val valuesMap: java.util.Map[String, String] = new java.util.HashMap[String, String]
		valuesMap.put("mappingpediaUsername", mappingpediaUsername)
		valuesMap.put("mappingDirectory", mappingDirectory)
		valuesMap.put("mappingFilename", mappingFilename)
		val sub: StrSubstitutor = new StrSubstitutor(valuesMap)
		val templateResultString: String = sub.replace(templateString)
		val githubRepo = MappingPediaProperties.githubRepo
		val mappingBlobURL: String = githubRepo + "/blob/master/" + templateResultString
		//val mappingBlobURL: String = "https://github.com/oeg-upm/mappingpedia-contents/blob/master/" + templateResultString
		logger.debug("mappingBlobURL = " + mappingBlobURL)
		properties.setMappingDocumentFilePath(mappingBlobURL)
		val outputFileName = if (pOutputFilename == null) {
			//"output.nt";
			MappingPediaConstant.DEFAULT_OUTPUT_FILENAME;
		} else {
			pOutputFilename;
		}
		val outputFilepath = "executions/" + templateResultString + "/" + outputFileName

		properties.setOutputFilePath(outputFilepath);
		properties.setCSVFile(datasetFile);
		properties.setQueryFilePath(queryFile);
		try {
			val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
			val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
			runner.run
			logger.info("mapping execution success!")
			val outputFile: File = new File(outputFilepath)
			val response = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser, MappingPediaProperties.githubAccessToken
				, mappingpediaUsername, mappingDirectory, outputFileName
				, "add mapping execution result by mappingpedia engine", outputFile);

			val responseStatus: Int = response.getStatus
			logger.info("responseStatus = " + responseStatus)
			val responseStatusText: String = response.getStatusText
			logger.info("responseStatusText = " + responseStatusText)
			if (HttpURLConnection.HTTP_CREATED== responseStatus || HttpURLConnection.HTTP_OK == responseStatus) {
				val outputGitHubURL: String = response.getBody.getObject.getJSONObject("content").getString("url");
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null
					,null , outputGitHubURL, responseStatusText, responseStatus)
				return executionResult
			}
			else {
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null
					, null, null, responseStatusText, responseStatus)
				return executionResult
			}
		}
		catch {
			case e: Exception => {
				e.printStackTrace
				val errorMessage: String = "Error occured: " + e.getMessage
				logger.error("mapping execution failed: " + errorMessage)
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null
					, null, null, errorMessage, HttpURLConnection.HTTP_INTERNAL_ERROR)
				return executionResult
			}
		}
	}

	def uploadNewMapping(mappingpediaUsername: String, manifestFileRef: MultipartFile, mappingFileRef: MultipartFile
											 , replaceMappingBaseURI: String): MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		// Path where the uploaded files will be stored.
		val uuid = UUID.randomUUID.toString
		logger.debug("uuid = " + uuid)
		this.uploadNewMapping(mappingpediaUsername, uuid, manifestFileRef, mappingFileRef, replaceMappingBaseURI);
	}

	def uploadNewMapping(mappingpediaUsername: String, datasetID: String
	, manifestFileRef: MultipartFile, mappingFileRef: MultipartFile , replaceMappingBaseURI: String
	) : MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

		val newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS + datasetID + "/"

		try {
			val manifestFilePath = if (manifestFileRef != null) {
				val manifestFile = MappingPediaUtility.multipartFileToFile(manifestFileRef, datasetID)
				manifestFile.getPath
			} else {
				null;
			}

			val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, datasetID)
			val mappingFilePath = mappingFile.getPath
			MappingPediaRunner.run(manifestFilePath, null, mappingFilePath, null, "false", Application.mappingpediaR2RML
				, replaceMappingBaseURI, newMappingBaseURI)

			val commitMessage = "add new mapping by mappingpedia-engine"
			val mappingContent = MappingPediaRunner.getMappingContent(manifestFilePath, null, mappingFilePath, null)
			val base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent)
			logger.info("storing mapping file in github ...")
			val response = GitHubUtility.putEncodedContent(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername, datasetID, mappingFile.getName
				, commitMessage, base64EncodedContent)
			logger.debug("response.getHeaders = " + response.getHeaders)
			logger.debug("response.getBody = " + response.getBody)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)
			val executionResult = if (HttpURLConnection.HTTP_CREATED == responseStatus) {
				val githubMappingURL = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("githubMappingURL = " + githubMappingURL)
				logger.info("mapping inserted.")
				new MappingPediaExecutionResult(manifestFilePath, null, githubMappingURL, null, null, responseStatusText, responseStatus)
			}
			else {
				new MappingPediaExecutionResult(manifestFilePath, null, null, null, null, responseStatusText, responseStatus)
			}
			executionResult;
		} catch {
			case e: Exception =>
				val errorMessage = e.getMessage
				logger.error("error uploading a new mapping file: " + errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new MappingPediaExecutionResult(null, null, null, null, null, errorMessage, errorCode)
				executionResult
		}
	}

	def getMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String):MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		val response = GitHubUtility.getFile(MappingPediaProperties.githubUser, MappingPediaProperties.githubAccessToken
			, mappingpediaUsername, mappingDirectory, mappingFilename)
		val responseStatus = response.getStatus
		logger.debug("responseStatus = " + responseStatus)
		val responseStatusText = response.getStatusText
		logger.debug("responseStatusText = " + responseStatusText)
		val executionResult = if (HttpURLConnection.HTTP_OK == responseStatus) {
			val githubMappingURL = response.getBody.getObject.getString("url")
			logger.debug("githubMappingURL = " + githubMappingURL)
			new MappingPediaExecutionResult(null, null, githubMappingURL, null, null, responseStatusText, responseStatus)
		} else {
			new MappingPediaExecutionResult(null, null, null, null, null, responseStatusText, responseStatus)
		}
		executionResult;
	}

	def updateExistingMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String, mappingFileRef:MultipartFile): MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		logger.debug("mappingFileRef = " + mappingFileRef)
		try {
			val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, mappingDirectory)
			val mappingFilePath = mappingFile.getPath
			logger.debug("mapping file path = " + mappingFilePath)
			val commitMessage = "Mapping modification by mappingpedia-engine.Application"
			val mappingContent = MappingPediaRunner.getMappingContent(null, null, mappingFilePath, null)
			val base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent)
			val response = GitHubUtility.putEncodedContent(MappingPediaProperties.githubUser, MappingPediaProperties.githubAccessToken, mappingpediaUsername, mappingDirectory, mappingFilename, commitMessage, base64EncodedContent)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)

			val executionResult = if (HttpURLConnection.HTTP_OK == responseStatus) {
				val githubMappingURL = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("githubMappingURL = " + githubMappingURL)
				new MappingPediaExecutionResult(null, null, githubMappingURL, null, null, responseStatusText, responseStatus)
			} else {
				new MappingPediaExecutionResult(null, null, null, null, null, responseStatusText, responseStatus)
			}
			executionResult;
		} catch {
			case e: Exception =>
				e.printStackTrace()
				val errorMessage = "error processing the uploaded mapping file: " + e.getMessage
				logger.error(errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new MappingPediaExecutionResult(null, null, null, null, null, errorMessage, errorCode)
				executionResult
		}
	}

	def storeRDFFile(fileRef: MultipartFile, graphURI: String): MappingPediaExecutionResult = {
		try {
			val file = MappingPediaUtility.multipartFileToFile(fileRef)
			val filePath = file.getPath
			logger.info("file path = " + filePath)
			MappingPediaUtility.store(filePath, graphURI)
			val errorCode = HttpURLConnection.HTTP_CREATED
			val status = "success, file uploaded to: " + filePath
			logger.info("file inserted.")
			val executionResult = new MappingPediaExecutionResult(null, null, filePath, null, null, status, errorCode)
			executionResult
		} catch {
			case e: Exception =>
				val errorMessage = "error processing uploaded file: " + e.getMessage
				logger.error(errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val status = "failed, error message = " + e.getMessage
				val executionResult = new MappingPediaExecutionResult(null, null, null, null, null, status, errorCode)
				executionResult
		}
	}
}
package es.upm.fi.dia.oeg.mappingpedia

import java.io.{BufferedWriter, File, FileWriter, InputStream}
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import be.ugent.mmlab.rml.config.RMLConfiguration
import be.ugent.mmlab.rml.core.{StdMetadataRMLEngine, StdRMLEngine}
import be.ugent.mmlab.rml.mapdochandler.extraction.std.StdRMLMappingFactory
import be.ugent.mmlab.rml.mapdochandler.retrieval.RMLDocRetrieval
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.model.{ListResult, MappingDocument, MappingPediaExecutionResult, OntologyClass}
import es.upm.fi.dia.oeg.mappingpedia.utility.{GitHubUtility, MappingPediaUtility}
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory}
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang.text.StrSubstitutor
import org.apache.jena.graph.Triple
import org.apache.jena.ontology.OntModel
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.impl.StatementImpl
import org.apache.jena.vocabulary.RDF
import org.apache.log4j.BasicConfigurator
import org.openrdf.rio.RDFFormat
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile
import virtuoso.jena.driver.{VirtModel, VirtuosoQueryExecutionFactory}

import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import org.apache.commons.io.{FileUtils, FilenameUtils}

//class MappingPediaR2RML(mappingpediaGraph:VirtGraph) {
class MappingPediaEngine() {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	var manifestModel:Model = null;
	var mappingDocumentModel:Model = null;
	var clearGraph:Boolean = false;

  def generateAdditionalTriples() : List[Triple] = {
    var newTriples:List[Triple] = List.empty;
    
    val r2rmlMappingDocumentResources = manifestModel.listResourcesWithProperty(
				RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_R2RMLMAPPINGDOCUMENT_CLASS);
		logger.info("r2rmlMappingDocumentResources = " + r2rmlMappingDocumentResources);

		if(r2rmlMappingDocumentResources != null) {
		  while(r2rmlMappingDocumentResources.hasNext()) {
  			val r2rmlMappingDocumentResource = r2rmlMappingDocumentResources.nextResource();
				logger.info("r2rmlMappingDocumentResource = " + r2rmlMappingDocumentResource);

				//improve this code using, get all x from ?x rr:LogicalTable ?lt
				//mapping documents do not always explicitly have a TriplesMap
  			//val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
//  				RDF.`type`, MappingPediaConstant.R2RML_TRIPLESMAP_CLASS);
				val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
					MappingPediaConstant.R2RML_LOGICALTABLE_PROPERTY);
				logger.info("triplesMapResources = " + triplesMapResources);
  			if(triplesMapResources != null) {
  			  while(triplesMapResources.hasNext()) {
  			    val triplesMapResource = triplesMapResources.nextResource();
  			    val newStatement = new StatementImpl(r2rmlMappingDocumentResource
							, MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMapResource);
  			    logger.info("adding new hasTriplesMap statement: " + newStatement);
  			    val newTriple = newStatement.asTriple();
  			    newTriples = newTriples ::: List(newTriple);
  			  }
  			}
		  }
		}

		newTriples;
  }



  
  //def getMappingpediaGraph() = this.mappingpediaGraph;

}

object MappingPediaEngine {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
	val schemaOrgModel:OntModel = MappingPediaUtility.loadSchemaOrgOntology();

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

/*	def uploadNewMapping(mappingpediaUsername: String, manifestFileRef: MultipartFile, mappingFileRef: MultipartFile
											 , replaceMappingBaseURI: String, generateManifestFile:String
											 , mappingDocumentTitle: String, mappingDocumentCreator:String, mappingDocumentSubjects:String
											 //, datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String
											): MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		// Path where the uploaded files will be stored.
		val uuid = UUID.randomUUID.toString
		logger.debug("uuid = " + uuid)
		this.uploadNewMapping(mappingpediaUsername, uuid, manifestFileRef, mappingFileRef, replaceMappingBaseURI
			, generateManifestFile, mappingDocumentTitle, mappingDocumentCreator, mappingDocumentSubjects
			//, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
		);
	}*/

	def uploadNewMapping(mappingpediaUsername: String, pDatasetID: String, manifestFileRef: MultipartFile
											 , mappingFileRef: MultipartFile , replaceMappingBaseURI: String, generateManifestFile:String
											 , mappingDocumentTitle: String, mappingDocumentCreator:String, mappingDocumentSubjects:String
											 //, datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String
											, pMappingLanguage:String

											) : MappingPediaExecutionResult = {
		val datasetID = if(pDatasetID == null) UUID.randomUUID.toString else pDatasetID;
		val mappingLanguage = if(pMappingLanguage == null) {
			MappingPediaConstant.MAPPING_LANGUAGE_R2RML
		} else {
			pMappingLanguage
		}

		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

		val newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS + datasetID + "/"
		val mappingDocumentID = UUID.randomUUID.toString

		try {

			//STORING MAPPING FILE ON GITHUB
			val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, datasetID)
			val mappingFilePath = mappingFile.getPath
			val commitMessage = "add a new mapping file by mappingpedia-engine"
			val mappingContent = MappingPediaEngine.getMappingContent(mappingFilePath)
			val base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent)
			logger.info("STORING MAPPING FILE ON GITHUB ...")
			val response = GitHubUtility.putEncodedContent(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername, datasetID, mappingFile.getName
				, commitMessage, base64EncodedContent)
			//logger.debug("response.getHeaders = " + response.getHeaders)
			//logger.debug("response.getBody = " + response.getBody)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)

			val mappingDocumentGitHubURL = if (HttpURLConnection.HTTP_OK == responseStatus
				|| HttpURLConnection.HTTP_CREATED == responseStatus) {
				val url = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.info("Mapping stored on GitHub")
				url;
			} else {
				logger.error("Error when storing mapping on GitHub: " + responseStatusText)
				null
			}

			logger.info("storing/creating manifest file ...")
			val manifestFile = if (manifestFileRef != null) {
				MappingPediaUtility.multipartFileToFile(manifestFileRef, datasetID)
			} else {
				if("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
					try {
						//GENERATE MANIFEST FILE IF NOT PROVIDED
						logger.info("GENERATING MANIFEST FILE ...")
						val templateFiles = List(
							MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA_NAMESPACE
							, MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA);

						val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

						val mapValues:Map[String,String] = Map(
							"$mappingDocumentID" -> mappingDocumentID
							, "$mappingDocumentTitle" -> mappingDocumentTitle
							, "$mappingDocumentDateTimeSubmitted" -> mappingDocumentDateTimeSubmitted
							, "$mappingDocumentCreator" -> mappingDocumentCreator
							, "$mappingDocumentSubjects" -> mappingDocumentSubjects
							, "$mappingDocumentFilePath" -> mappingDocumentGitHubURL
							, "$datasetID" -> datasetID
							, "$mappingLanguage" -> mappingLanguage

							//, "$datasetTitle" -> datasetTitle
							//, "$datasetKeywords" -> datasetKeywords
							//, "$datasetPublisher" -> datasetPublisher
							//, "$datasetLanguage" -> datasetLanguage
						);

						val filename = "metadata-mappingdocument.ttl";
						MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, datasetID);
					} catch {
						case e:Exception => {
							e.printStackTrace();
							val errorMessage = "Error occured when generating manifest file: " + e.getMessage;
							logger.error(errorMessage)
							null
						}
					}
				} else {
					null
				}
			}
			val manifestFilePath = manifestFile.getPath;
			logger.debug("manifestFilePath = " + manifestFilePath)

			//STORING MAPPING AND MANIFEST FILES ON VIRTUOSO
			logger.info("STORING MAPPING AND MANIFEST FILES ON VIRTUOSO ...")
			MappingPediaRunner.run(manifestFilePath,mappingFilePath, "false", Application.mappingpediaEngine
				, replaceMappingBaseURI, newMappingBaseURI)
			logger.info("Mapping and manifest file stored on Virtuoso")


			//STORING MANIFEST FILE ON GITHUB
			logger.info("STORING MANIFEST FILE ON GITHUB ...")
			val addNewManifestCommitMessage = "Add a new manifest file by mappingpedia-engine"
			val addNewManifestResponse = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername
				, datasetID, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
			val addNewManifestResponseStatus = addNewManifestResponse.getStatus
			val addNewManifestResponseStatusText = addNewManifestResponse.getStatusText

			val manifestGitHubURL = if (HttpURLConnection.HTTP_CREATED == addNewManifestResponseStatus
				|| HttpURLConnection.HTTP_OK == addNewManifestResponseStatus) {
				logger.info("Manifest file stored on GitHub")
				addNewManifestResponse.getBody.getObject.getJSONObject("content").getString("url")
			} else {
				logger.info("Error occured when storing manifest file on GitHub: " + addNewManifestResponseStatusText)
				null
			}


			new MappingPediaExecutionResult(manifestGitHubURL, null, mappingDocumentGitHubURL
				, null, null, "OK", HttpURLConnection.HTTP_OK)

		} catch {
			case e: Exception =>
				val errorMessage = e.getMessage
				logger.error("error uploading a new mapping file: " + errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new MappingPediaExecutionResult(null, null, null, null, null, errorMessage, errorCode)
				executionResult
		}


	}

	def addDatasetFile(datasetFileRef: MultipartFile, manifestFileRef:MultipartFile, generateManifestFile:String, mappingpediaUsername:String
		, datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String
		, distributionAccessURL:String, distributionDownloadURL:String, distributionMediaType:String
	) : MappingPediaExecutionResult = {
		val datasetID = UUID.randomUUID.toString;
		this.addDatasetFileWithID(datasetFileRef, manifestFileRef, generateManifestFile, mappingpediaUsername
			, datasetID, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage
			, distributionAccessURL, distributionDownloadURL, distributionMediaType
		);
	}

	def addDatasetFileWithID(datasetFileRef: MultipartFile, manifestFileRef:MultipartFile, generateManifestFile:String, mappingpediaUsername:String
		, datasetID:String
		, datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String
		, pDistributionAccessURL:String, pDistributionDownloadURL:String, distributionMediaType:String
	) : MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

		var distributionAccessURL = pDistributionAccessURL;
		if(distributionAccessURL != null && !distributionAccessURL.startsWith("<")) {
			distributionAccessURL = "<" + distributionAccessURL;
		}
		if(distributionAccessURL != null && !distributionAccessURL.endsWith(">")) {
			distributionAccessURL = distributionAccessURL + ">";
		}
		var distributionDownloadURL = pDistributionDownloadURL;
		if(distributionDownloadURL != null && !distributionDownloadURL.startsWith("<")) {
			distributionDownloadURL = "<" + distributionDownloadURL;
		}
		if(distributionDownloadURL != null && !distributionDownloadURL.endsWith(">")) {
			distributionDownloadURL = distributionDownloadURL + ">";
		}

		try {
			val manifestFile:File = if (manifestFileRef != null) {
				MappingPediaUtility.multipartFileToFile(manifestFileRef, datasetID)
			} else {
				//GENERATE MANIFEST FILE IF NOT PROVIDED
				if("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
					logger.info("generating manifest file ...")
					try {
						val templateFiles = List(
							"templates/metadata-namespaces-template.ttl"
							, "templates/metadata-dataset-template.ttl"
							, "templates/metadata-distributions-template.ttl"
						);

						val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

						val mapValues:Map[String,String] = Map(
							"$datasetID" -> datasetID
							, "$datasetTitle" -> datasetTitle
							, "$datasetKeywords" -> datasetKeywords
							, "$datasetPublisher" -> datasetPublisher
							, "$datasetLanguage" -> datasetLanguage
							, "$distributionID" -> datasetID
							, "$distributionAccessURL" -> distributionAccessURL
							, "$distributionDownloadURL" -> distributionDownloadURL
							, "$distributionMediaType" -> distributionMediaType
						);

						val filename = "metadata-dataset.ttl";
						MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, datasetID);
					} catch {
						case e:Exception => {
							e.printStackTrace()
							val errorMessage = "Error occured when generating manifest file: " + e.getMessage
							null;
						}
					}
				} else {
					null
				}
			}

			if(manifestFile != null) {
				logger.info("storing the manifest-dataset triples on virtuoso ...")
				logger.debug("manifestFile = " + manifestFile);
				MappingPediaUtility.store(manifestFile, MappingPediaProperties.graphName)

			}



			val optionDatasetFile:Option[File] = if(datasetFileRef == null) {
				None
			}  else {
				Some(MappingPediaUtility.multipartFileToFile(datasetFileRef, datasetID))
			}


			var datasetURL:String = null;
			val addNewDatasetResponseStatus = if(optionDatasetFile.isDefined) {
				logger.info("storing a new dataset file on github ...")
				val datasetFile = optionDatasetFile.get;
				val addNewDatasetCommitMessage = "Add a new dataset file by mappingpedia-engine"
				val addNewDatasetResponse = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser
					, MappingPediaProperties.githubAccessToken, mappingpediaUsername
					, datasetID, datasetFile.getName, addNewDatasetCommitMessage, datasetFile)
				val addNewDatasetResponseStatus = addNewDatasetResponse.getStatus

				if (HttpURLConnection.HTTP_CREATED == addNewDatasetResponseStatus) {
					datasetURL = addNewDatasetResponse.getBody.getObject.getJSONObject("content").getString("url")
				}
				addNewDatasetResponseStatus;
			} else {
				HttpURLConnection.HTTP_OK;
			}



			val manifestURL:String = if(manifestFile == null) {
				null
			} else {
				logger.info("storing manifest-dataset file on github ...")
				val addNewManifestCommitMessage = "Add a new manifest file by mappingpedia-engine"
				val addNewManifestResponse = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser
					, MappingPediaProperties.githubAccessToken, mappingpediaUsername
					, datasetID, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
				val addNewManifestResponseStatus = addNewManifestResponse.getStatus
				logger.info("addNewManifestResponseStatus = " + addNewManifestResponseStatus)

				if (HttpURLConnection.HTTP_CREATED == addNewManifestResponseStatus) {
					addNewManifestResponse.getBody.getObject.getJSONObject("content").getString("url")
				} else {
					null
				}
			}



			if(HttpURLConnection.HTTP_CREATED == addNewDatasetResponseStatus || HttpURLConnection.HTTP_OK == addNewDatasetResponseStatus) {
				val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
					, null, null, "OK", HttpURLConnection.HTTP_OK)
				return executionResult;
			} else {
				val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
					, null, null, "Internal Error", HttpURLConnection.HTTP_INTERNAL_ERROR)
				return executionResult;
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

	@throws(classOf[Exception])
	def executeMapping2(mappingURL: String, pMappingLanguage:String
											, datasetDistributionURL: String, fieldSeparator:String
										 , queryFile:String, pOutputFilename: String) : MappingPediaExecutionResult = {
		val mappingLanguage = if(pMappingLanguage == null) {
			MappingPediaConstant.MAPPING_LANGUAGE_R2RML
		} else {
			pMappingLanguage
		}

		val mappingpediaUsername = "executions"
		val mappingDirectory = UUID.randomUUID.toString
		val outputFileName = if (pOutputFilename == null) {
			//"output.nt";
			//MappingPediaConstant.DEFAULT_OUTPUT_FILENAME;
			UUID.randomUUID.toString
		} else {
			pOutputFilename;
		}
		val outputFilepath = "executions/" + mappingDirectory + "/" + outputFileName

		try {
			if(MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {

				val properties: MorphCSVProperties = new MorphCSVProperties
				properties.setDatabaseName(mappingpediaUsername + "/" + mappingDirectory)
				properties.setMappingDocumentFilePath(mappingURL)
				properties.setOutputFilePath(outputFilepath);
				properties.setCSVFile(datasetDistributionURL);
				properties.setQueryFilePath(queryFile);
				if(fieldSeparator != null) {
					properties.fieldSeparator = Some(fieldSeparator);
				}

				val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
				val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
				runner.run
			} else if(MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
				val rmlConnector = new RMLMapperConnector();
				//rmlConnector.execute(mappingURL, outputFilepath);
				rmlConnector.executeWithMain(datasetDistributionURL, mappingURL, outputFilepath);

			} else if(MappingPediaConstant.MAPPING_LANGUAGE_xR2RML.equalsIgnoreCase(mappingLanguage)) {
				throw new Exception(mappingLanguage + " Language is not supported yet");
			} else {
				throw new Exception(mappingLanguage + " Language is not supported yet");
			}

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
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, datasetDistributionURL, mappingURL
					,queryFile , outputGitHubURL, responseStatusText, responseStatus)
				return executionResult
			}
			else {
				val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, datasetDistributionURL, mappingURL
					, queryFile, null, responseStatusText, responseStatus)
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

	def executeMapping(mappingpediaUsername:String, mappingDirectory: String
										 , mappingFilename: String, datasetFile: String
										 , queryFile:String, pOutputFilename: String) : MappingPediaExecutionResult = {
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
		logger.debug("datasetFile = " + datasetFile)

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

	def generateStringFromTemplateFile(map: Map[String, String], templateFilePath:String) : String = {
		try {

			//var lines: String = Source.fromResource(templateFilePath).getLines.mkString("\n");
			val templateStream: InputStream = getClass.getResourceAsStream("/" + templateFilePath)
			val templateLines = scala.io.Source.fromInputStream(templateStream).getLines.mkString("\n");

			val mappingDocumentLines = map.foldLeft(templateLines)( (acc, kv) => {
				val mapValue:String = map.get(kv._1).getOrElse("");

				logger.debug("replacing " + kv._1 + " with " + mapValue);
				acc.replaceAllLiterally(kv._1, mapValue)
			});


			/*
			var lines3 = lines;
			map.keys.foreach(key => {
				lines3 = lines3.replaceAllLiterally(key, map(key));
      })
			logger.info("lines3 = " + lines3)
			*/

			mappingDocumentLines;
		} catch {
			case e:Exception => {
				logger.error("error generating manifest lines: " + e.getMessage);
				e.printStackTrace();
				val templateLines="";
				templateLines;
			}
		}
	}

	def generateManifestFile(map: Map[String, String], templateFiles:List[String], filename:String, datasetID:String) : File = {
		try {
			val manifestTriples = templateFiles.foldLeft("") { (z, i) => {
				logger.debug("generating manifest triples from:" + i)
				z + "\n" + this.generateStringFromTemplateFile(map, i);
			} }
			logger.debug("manifestTriples = " + manifestTriples)

			//def mappingDocumentLines = this.generateManifestLines(map, "templates/metadata-mappingdocument-template.ttl");
			//logger.debug("mappingDocumentLines = " + mappingDocumentLines)

			//def datasetLines = this.generateManifestLines(map, "templates/metadata-dataset-template.ttl");
			//logger.debug("datasetLines = " + datasetLines)

			val uploadDirectoryPath: String = MappingPediaConstant.DEFAULT_UPLOAD_DIRECTORY;
			val outputDirectory: File = new File(uploadDirectoryPath)
			if (!outputDirectory.exists) {
				outputDirectory.mkdirs
			}
			val uuidDirectoryPath: String = uploadDirectoryPath + "/" + datasetID
			//logger.info("upload directory path = " + uuidDirectoryPath)
			val uuidDirectory: File = new File(uuidDirectoryPath)
			if (!uuidDirectory.exists) {
				uuidDirectory.mkdirs
			}

			val file = new File(uuidDirectory + "/" + filename)
			val bw = new BufferedWriter(new FileWriter(file))
			bw.write(manifestTriples)
			bw.close()
			file
		} catch {
			case e:Exception => {
				logger.error("error generating manifest file: " + e.getMessage);
				e.printStackTrace();
				null

			}
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

	def updateExistingMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
														, mappingFileRef:MultipartFile): MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		logger.debug("mappingFileRef = " + mappingFileRef)
		try {
			val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, mappingDirectory)
			val mappingFilePath = mappingFile.getPath
			logger.debug("mapping file path = " + mappingFilePath)
			val commitMessage = "Mapping modification by mappingpedia-engine.Application"
			val mappingContent = MappingPediaEngine.getMappingContent(null, null, mappingFilePath, null)
			val base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent)
			val response = GitHubUtility.putEncodedContent(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername, mappingDirectory, mappingFilename
				, commitMessage, base64EncodedContent)
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

	def getManifestContent(manifestFilePath:String, manifestText:String):String = {
		logger.info("reading manifest  ...");
		val manifestContent:String = if(manifestText == null) {
			if(manifestFilePath == null) {
				val errorMessage = "no manifest is provided";
				logger.error(errorMessage);
				throw new Exception(errorMessage);
			} else {
				val manifestFileContent = fromFile(manifestFilePath).getLines.mkString("\n");
				//logger.info("manifestFileContent = \n" + manifestFileContent);
				manifestFileContent;
			}
		} else {
			manifestText;
		}
		manifestContent;
	}

	def getManifestContent(manifestFilePath:String):String = {
		this.getManifestContent(manifestFilePath, null);
	}

	def getMappingContent(manifestFilePath:String, pMappingFilePath:String):String = {
		this.getMappingContent(manifestFilePath, null, pMappingFilePath:String, null)
	}

	def getMappingContent(pMappingFilePath:String):String = {
		val mappingFileContent = fromFile(pMappingFilePath).getLines.mkString("\n");
		mappingFileContent;
	}

	def getMappingContent(manifestFilePath:String, manifestText:String, pMappingFilePath:String, pMappingText:String):String = {
		logger.info("reading r2rml file ...");
		val mappingContent:String = if(pMappingText == null) {
			val mappingFilePath = if(pMappingFilePath == null) {
				val mappingFilePathFromManifest = MappingPediaEngine.getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath);
				mappingFilePathFromManifest;
			}  else {
				pMappingFilePath;
			}

			val mappingFileContent = fromFile(mappingFilePath).getLines.mkString("\n");
			//logger.info("mappingFileContent = \n" + mappingFileContent);
			mappingFileContent;
		} else {
			pMappingText;
		}
		mappingContent;
	}

	def getAllTriplesMaps() : ListResult = {
		val prolog = "PREFIX rr: <http://www.w3.org/ns/r2rml#> \n"
		var queryString: String = prolog + "SELECT ?tm \n";
		queryString = queryString + " FROM <" + MappingPediaProperties.graphName + ">\n";
		queryString = queryString + " WHERE {?tm rr:logicalTable ?lt} \n";

		val m = VirtModel.openDatabaseModel(MappingPediaProperties.graphName, MappingPediaProperties.virtuosoJDBC
			, MappingPediaProperties.virtuosoUser, MappingPediaProperties.virtuosoPwd);

		logger.debug("Executing query=\n" + queryString)

		val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
		var results:List[String] = List.empty;
		try {
			val rs = qexec.execSelect
			while (rs.hasNext) {
				val rb = rs.nextSolution
				val rdfNode = rb.get("tm");
				val rdfNodeInString = rb.get("tm").toString;
				results = rdfNodeInString :: results;
			}
		} finally qexec.close

		val listResult = new ListResult(results.length, results);
		listResult
	}

	def findAllMappingDocuments() : ListResult = {

		//val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
		val mapValues:Map[String,String] = Map(
			"$graphURL" -> MappingPediaProperties.graphName
		);

		val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
			mapValues, "templates/findAllMappingDocuments.rq")

		this.findMappingDocuments(queryString);

/*		val m = VirtModel.openDatabaseModel(MappingPediaProperties.graphName, MappingPediaProperties.virtuosoJDBC
			, MappingPediaProperties.virtuosoUser, MappingPediaProperties.virtuosoPwd);

		logger.debug("Executing query=\n" + queryString)

		val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
		var results:List[MappingDocument] = List.empty;
		try {
			val rs = qexec.execSelect
			while (rs.hasNext) {
				val qs = rs.nextSolution
				val id = MappingPediaUtility.getStringOrElse(qs, "md", null);
				val title = MappingPediaUtility.getStringOrElse(qs, "title", null);
				val dataset = MappingPediaUtility.getStringOrElse(qs, "dataset", null);
				val filePath = MappingPediaUtility.getStringOrElse(qs, "filePath", null);
				val creator = MappingPediaUtility.getStringOrElse(qs, "creator", null);
				val distribution = MappingPediaUtility.getStringOrElse(qs, "distribution", null);
				val distributionAccessURL = MappingPediaUtility.getStringOrElse(qs, "accessURL", null);
				val mappingDocumentURL = MappingPediaUtility.getStringOrElse(qs, "mappingDocumentURL", null);
				val mappingLanguage = MappingPediaUtility.getOptionString(qs, "mappingLanguage");

				val md = new MappingDocument(id, title, dataset, filePath, creator, distribution
					, distributionAccessURL, mappingDocumentURL, mappingLanguage);
				results = md :: results;
			}
		} finally qexec.close

		val listResult = new ListResult(results.length, results);
		listResult*/
	}

	def findMappingDocumentsByMappedClass(mappedClass:String) : ListResult = {
		val queryTemplateFile = "templates/findTriplesMapsByMappedClass.rq";

		val mapValues:Map[String,String] = Map(
			"$graphURL" -> MappingPediaProperties.graphName
			, "$mappedClass" -> mappedClass
			//, "$mappedProperty" -> mappedProperty
		);

		val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
		this.findMappingDocuments(queryString);
	}

	def findMappingDocumentsByMappedProperty(mappedProperty:String) : ListResult = {
		val queryTemplateFile = "templates/findTriplesMapsByMappedProperty.rq";

		val mapValues:Map[String,String] = Map(
			"$graphURL" -> MappingPediaProperties.graphName
		, "$mappedProperty" -> mappedProperty
		);

		val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
		this.findMappingDocuments(queryString);
	}

	def findMappingDocumentsByMappedColumn(mappedColumn:String) : ListResult = {
		val queryTemplateFile = "templates/findTriplesMapsByMappedColumn.rq";

		val mapValues:Map[String,String] = Map(
			"$graphURL" -> MappingPediaProperties.graphName
			, "$mappedColumn" -> mappedColumn
		);

		val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
		this.findMappingDocuments(queryString);
	}

	def findMappingDocumentsByMappedTable(mappedTable:String) : ListResult = {
		val queryTemplateFile = "templates/findTriplesMapsByMappedTable.rq";

		val mapValues:Map[String,String] = Map(
			"$graphURL" -> MappingPediaProperties.graphName
			, "$mappedTable" -> mappedTable
		);

		val queryString:String =	MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
		this.findMappingDocuments(queryString);
	}

	def findMappingDocuments(searchType:String, searchTerm:String) : ListResult = {
		val result:ListResult = if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_CLASS.equals(searchType) && searchTerm != null) {
			logger.info("findMappingDocumentsByMappedClass:" + searchTerm)
			val listResult = MappingPediaEngine.findMappingDocumentsByMappedClass(searchTerm)
			listResult
		} else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_PROPERTY.equals(searchType) && searchTerm != null) {
			logger.info("findMappingDocumentsByMappedProperty:" + searchTerm)
			val listResult = MappingPediaEngine.findMappingDocumentsByMappedProperty(searchTerm)
			listResult
		} else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_TABLE.equals(searchType) && searchTerm != null) {
			logger.info("findMappingDocumentsByMappedTable:" + searchTerm)
			val listResult = MappingPediaEngine.findMappingDocumentsByMappedTable(searchTerm)
			listResult
		} else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_COLUMN.equals(searchType) && searchTerm != null) {
			logger.info("findMappingDocumentsByMappedColumn:" + searchTerm)
			val listResult = MappingPediaEngine.findMappingDocumentsByMappedColumn(searchTerm)
			listResult
		} else {
			logger.info("findAllMappingDocuments")
			val listResult = MappingPediaEngine.findAllMappingDocuments
			listResult
		}
		logger.info("result = " + result)

		result;
	}

	def findMappingDocuments(queryString:String) : ListResult = {
		val m = VirtModel.openDatabaseModel(MappingPediaProperties.graphName, MappingPediaProperties.virtuosoJDBC
			, MappingPediaProperties.virtuosoUser, MappingPediaProperties.virtuosoPwd);

		logger.debug("Executing query=\n" + queryString)

		val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
		var results:List[MappingDocument] = List.empty;
		try {
			val rs = qexec.execSelect
			while (rs.hasNext) {
				val qs = rs.nextSolution
				val id = MappingPediaUtility.getStringOrElse(qs, "md", null);
				val title = MappingPediaUtility.getStringOrElse(qs, "title", null);
				val dataset = MappingPediaUtility.getStringOrElse(qs, "dataset", null);
				val filePath = MappingPediaUtility.getStringOrElse(qs, "filePath", null);
				val creator = MappingPediaUtility.getStringOrElse(qs, "creator", null);
				val distribution = MappingPediaUtility.getStringOrElse(qs, "distribution", null);
				val distributionAccessURL = MappingPediaUtility.getStringOrElse(qs, "accessURL", null);
				val mappingDocumentURL = MappingPediaUtility.getStringOrElse(qs, "mappingDocumentURL", null);
				val mappingLanguage = MappingPediaUtility.getOptionString(qs, "mappingLanguage");


				val md = new MappingDocument(id, title, dataset, filePath, creator, distribution
					, distributionAccessURL, mappingDocumentURL, mappingLanguage);
				results = md :: results;
			}
		} finally qexec.close

		val listResult = new ListResult(results.length, results);
		listResult
	}

	def getSchemaOrgSubclasses(aClass:String, outputType:String, inputType:String) : ListResult = {
		MappingPediaUtility.getSubclasses(aClass, this.schemaOrgModel, outputType, inputType);

	}

	def getInstances(aClass:String, outputType:String, inputType:String) : ListResult = {
		val subclassesListResult = MappingPediaUtility.getSubclasses(aClass, this.schemaOrgModel, outputType, inputType);
		val subclassesInList:List[String] = subclassesListResult.results.map(result => result.asInstanceOf[OntologyClass].aClass).distinct
		logger.debug("subclassesInList" + subclassesInList)
		//new ListResult(subclassesInList.size, subclassesInList);
		val queryFile:String = null;

		val mappingDocuments:List[Object] = subclassesInList.flatMap(subclass =>
			MappingPediaEngine.findMappingDocumentsByMappedClass(subclass).getResults())


		val executionResults:List[String] = mappingDocuments.map(mappingDocument => {
			val md = mappingDocument.asInstanceOf[MappingDocument];
			val mappingLanguage = md.mappingLanguage.getOrElse(null);
			val distributionFieldSeparator = if(md.distributionFieldSeparator != null && md.distributionFieldSeparator.isDefined) {
				md.distributionFieldSeparator.get
			} else {
				null
			}
			val outputFilename = UUID.randomUUID.toString + ".nt"
			val executionResult = MappingPediaEngine.executeMapping(
				md.mappingDocumentDownloadURL, mappingLanguage
				, md.distributionAccessURL, distributionFieldSeparator
				, queryFile, outputFilename);


			executionResult.mappingExecutionResultDownloadURL;
			//mappingDocumentURL + " -- " + datasetDistributionURL
		})
		new ListResult(executionResults.size, executionResults);

	}

	/**
		*
		* @param commandLine
		* @return
		*         code taken from https://github.com/RMLio/RML-Processor/blob/ab26dac414692b3235164b271b376304869225ca/src/main/java/be/ugent/mmlab/rml/main/Main.java
		*/
	def retrieveParameters(commandLine:CommandLine): Map[String, String] = {
		val parameters:Map[String, String] = Map.empty;
		var parameterKeyValue:Array[String] = null
		val parameter:String = commandLine.getOptionValue("p", null)
		val subParameters:Array[String] = parameter.split(",")
		for (subParameter <- subParameters) {
			parameterKeyValue = subParameter.split("=")
			val key = parameterKeyValue(0)
			val value = parameterKeyValue(1)
			parameters.put(key, value)
		}
		parameters
	}
}
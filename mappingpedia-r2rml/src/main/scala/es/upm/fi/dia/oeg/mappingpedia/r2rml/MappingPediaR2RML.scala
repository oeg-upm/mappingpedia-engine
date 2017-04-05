package es.upm.fi.dia.oeg.mappingpedia.r2rml

import java.io.{BufferedWriter, File, FileWriter, InputStream}
import java.net.HttpURLConnection

import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory}
import org.apache.commons.lang.text.StrSubstitutor
import org.apache.jena.vocabulary.RDF
import org.apache.jena.graph.Triple
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.LogManager
import virtuoso.jena.driver.{VirtGraph, VirtModel, VirtuosoQueryExecutionFactory}
import java.util.{Date, UUID}

import org.apache.jena.rdf.model.{Model, RDFNode, Resource}
import org.apache.jena.rdf.model.impl.StatementImpl
import org.springframework.web.multipart.MultipartFile

import scala.io.Source.fromFile
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.io.Source
import java.text.SimpleDateFormat

//class MappingPediaR2RML(mappingpediaGraph:VirtGraph) {
class MappingPediaR2RML() {
	val logger : Logger = LogManager.getLogger(this.getClass);
	var manifestModel:Model = null;
	var mappingDocumentModel:Model = null;
	var clearGraph:Boolean = false;


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

object MappingPediaR2RML {
  val logger : Logger = LogManager.getLogger("MappingPediaR2RML");
	val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

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

	def addDatasetFile(datasetFileRef: MultipartFile, manifestFileRef:MultipartFile, generateManifestFile:String, mappingpediaUsername:String
										 , datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String
										) : MappingPediaExecutionResult = {
		val datasetID = UUID.randomUUID.toString;
		this.addDatasetFile(datasetFileRef, manifestFileRef, generateManifestFile, mappingpediaUsername
			, datasetID, datasetTitle, datasetKeywords, datasetPublisher, datasetLanguage);
	}

	def addDatasetFile(datasetFileRef: MultipartFile, manifestFileRef:MultipartFile, generateManifestFile:String, mappingpediaUsername:String
		, datasetID:String, datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String
	) : MappingPediaExecutionResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

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
							, "templates/metadata-dataset-template.ttl");

						val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

						val mapValues:Map[String,String] = Map(
							"$datasetID" -> datasetID
							, "$datasetTitle" -> datasetTitle
							, "$datasetKeywords" -> datasetKeywords
							, "$datasetPublisher" -> datasetPublisher
							, "$datasetLanguage" -> datasetLanguage
						);

						val filename = "metadata-dataset.ttl";
						MappingPediaR2RML.generateManifestFile(mapValues, templateFiles, filename, datasetID);
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

			logger.info("storing the manifest-dataset triples on virtuoso ...")
			MappingPediaUtility.store(manifestFile, MappingPediaProperties.graphName)



			val datasetFile:File = MappingPediaUtility.multipartFileToFile(datasetFileRef, datasetID)

			logger.info("storing a new dataset file on github ...")
			var datasetURL:String = null;
			val addNewDatasetCommitMessage = "Add a new dataset file by mappingpedia-engine"
			val addNewDatasetResponse = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername
				, datasetID, datasetFile.getName, addNewDatasetCommitMessage, datasetFile)
			val addNewDatasetResponseStatus = addNewDatasetResponse.getStatus

			if (HttpURLConnection.HTTP_CREATED == addNewDatasetResponseStatus) {
				datasetURL = addNewDatasetResponse.getBody.getObject.getJSONObject("content").getString("url")
			}

			logger.info("storing manifest-dataset file on github ...")
			var manifestURL:String = null;
			val addNewManifestCommitMessage = "Add a new manifest file by mappingpedia-engine"
			val addNewManifestResponse = GitHubUtility.putEncodedFile(MappingPediaProperties.githubUser
				, MappingPediaProperties.githubAccessToken, mappingpediaUsername
				, datasetID, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
			val addNewManifestResponseStatus = addNewManifestResponse.getStatus

			if (HttpURLConnection.HTTP_CREATED == addNewManifestResponseStatus) {
				manifestURL = addNewManifestResponse.getBody.getObject.getJSONObject("content").getString("url")
			}

			if(HttpURLConnection.HTTP_CREATED == addNewManifestResponseStatus
				&& HttpURLConnection.HTTP_CREATED == addNewDatasetResponseStatus) {
				val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
					, null, null, HttpURLConnection.HTTP_OK.toString, HttpURLConnection.HTTP_OK)
				return executionResult;
			} else {
				val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
					, null, null, HttpURLConnection.HTTP_INTERNAL_ERROR.toString, HttpURLConnection.HTTP_INTERNAL_ERROR)
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

	def generateManifestLines(map: Map[String, String], templateFilePath:String) : String = {
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
				z + "\n" + this.generateManifestLines(map, i);
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

	def uploadNewMapping(mappingpediaUsername: String, manifestFileRef: MultipartFile, mappingFileRef: MultipartFile
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
	}

	def uploadNewMapping(mappingpediaUsername: String, datasetID: String, manifestFileRef: MultipartFile
		, mappingFileRef: MultipartFile , replaceMappingBaseURI: String, generateManifestFile:String
		, mappingDocumentTitle: String, mappingDocumentCreator:String, mappingDocumentSubjects:String
		//, datasetTitle:String, datasetKeywords:String, datasetPublisher:String, datasetLanguage:String

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
			logger.debug("manifestFilePath = " + manifestFilePath)
			logger.debug("generateManifestFile = " + generateManifestFile)


			//STORING MAPPING FILE ON VIRTUOSO
			val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, datasetID)
			val mappingFilePath = mappingFile.getPath
			MappingPediaRunner.run(manifestFilePath,mappingFilePath, "false", Application.mappingpediaR2RML
				, replaceMappingBaseURI, newMappingBaseURI)

			//STORING MAPPING FILE ON GITHUB
			val commitMessage = "add a new mapping file by mappingpedia-engine"
			val mappingContent = MappingPediaR2RML.getMappingContent(manifestFilePath, null, mappingFilePath, null)
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
			var mappingDocumentFilePath = "";

			val uploadMappingToGitHubExecutionResult = if (HttpURLConnection.HTTP_CREATED == responseStatus) {
				mappingDocumentFilePath = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("githubMappingURL = " + mappingDocumentFilePath)
				logger.info("mapping inserted.")
				new MappingPediaExecutionResult(manifestFilePath, null, mappingDocumentFilePath, null, null, responseStatusText, responseStatus)
			}
			else {
				new MappingPediaExecutionResult(manifestFilePath, null, null, null, null, responseStatusText, responseStatus)
			}

			//GENERATE MANIFEST FILE IF NOT PROVIDED
			logger.info("generating manifest file ...")
			if(manifestFilePath == null) {
				if("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
					val templateFiles = List(
						"templates/metadata-namespaces-template.ttl"
						, "templates/metadata-mappingdocument-template.ttl");

					val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

					val mapValues:Map[String,String] = Map(
						"$mappingDocumentID" -> datasetID
						, "$mappingDocumentTitle" -> mappingDocumentTitle
						, "$mappingDocumentDateTimeSubmitted" -> mappingDocumentDateTimeSubmitted
						, "$mappingDocumentCreator" -> mappingDocumentCreator
						, "$mappingDocumentSubjects" -> mappingDocumentSubjects
						, "$mappingDocumentFilePath" -> mappingDocumentFilePath
						//, "$datasetTitle" -> datasetTitle
						//, "$datasetKeywords" -> datasetKeywords
						//, "$datasetPublisher" -> datasetPublisher
						//, "$datasetLanguage" -> datasetLanguage
					);

					val filename = "metadata-mappingdocument";
					MappingPediaR2RML.generateManifestFile(mapValues, templateFiles, filename, datasetID);
				}

			}

			uploadMappingToGitHubExecutionResult;

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
			val mappingContent = MappingPediaR2RML.getMappingContent(null, null, mappingFilePath, null)
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

	def getMappingContent(manifestFilePath:String, manifestText:String, pMappingFilePath:String, pMappingText:String):String = {
		logger.info("reading r2rml file ...");
		val mappingContent:String = if(pMappingText == null) {
			val mappingFilePath = if(pMappingFilePath == null) {
				val mappingFilePathFromManifest = MappingPediaR2RML.getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath);
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


}
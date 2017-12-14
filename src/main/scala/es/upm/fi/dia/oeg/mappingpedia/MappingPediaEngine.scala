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
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaRunner.logger
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.controller.{MappingDocumentController, MappingExecutionController}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility._
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
import virtuoso.jena.driver.{VirtGraph, VirtModel, VirtuosoQueryExecutionFactory}

import scala.collection.JavaConversions._
import scala.io.Source.fromFile
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.eclipse.egit.github.core.client.GitHubClient






//class MappingPediaR2RML(mappingpediaGraph:VirtGraph) {
//class MappingPediaEngine() {

object MappingPediaEngine {
	val logger: Logger = LoggerFactory.getLogger(this.getClass);
	val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
	var ontologyModel:OntModel = null;
	var mappingpediaProperties:MappingPediaProperties = null;
	var githubClient:GitHubUtility = null;
	var ckanClient:CKANUtility = null;
	var virtuosoClient:VirtuosoClient = null;
	var jenaClient:JenaClient = null;

	def getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath:String) : String = {
		logger.info("Reading manifest file : " + manifestFilePath);

		val manifestModel = this.virtuosoClient.readModelFromFile(manifestFilePath, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);

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








	def generateStringFromTemplateFile(map: Map[String, String], templateFilePath:String) : String = {
		//logger.info(s"Generating string from template file: $templateFilePath ...")
		try {

			//var lines: String = Source.fromResource(templateFilePath).getLines.mkString("\n");
			val templateStream: InputStream = getClass.getResourceAsStream("/" + templateFilePath)
			val templateLines = scala.io.Source.fromInputStream(templateStream).getLines.mkString("\n");

			val generatedLines = map.foldLeft(templateLines)( (acc, kv) => {
				val mapValue:String = map.get(kv._1).getOrElse("");
				if(mapValue ==null){
					logger.warn("the input value for " + kv._1 + " is null");
					acc.replaceAllLiterally(kv._1, "")
				} else {
					acc.replaceAllLiterally(kv._1, mapValue)
				}
				//logger.info("replacing " + kv._1 + " with " + mapValue);

			});


			/*
			var lines3 = lines;
			map.keys.foreach(key => {
				lines3 = lines3.replaceAllLiterally(key, map(key));
      })
			logger.info("lines3 = " + lines3)
			*/

			//logger.info(s"String from template file $templateFilePath generated.")
			generatedLines;
		} catch {
			case e:Exception => {
				logger.error("error generating file from template: " + e.getMessage);
				e.printStackTrace();
				throw e
			}
		}
	}

	def generateManifestFile(map: Map[String, String], templateFiles:List[String], filename:String, datasetID:String) : File = {
		try {
			val manifestTriples = templateFiles.foldLeft("") { (z, i) => {
				//logger.info("templateFiles.foldLeft" + (z, i))
				z + this.generateStringFromTemplateFile(map, i) + "\n\n" ;
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
			logger.debug(s"manifestTriples = $manifestTriples")
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






	def storeRDFFile(fileRef: MultipartFile, graphURI: String): GeneralResult = {
		try {
			val file = MappingPediaUtility.multipartFileToFile(fileRef)
			val filePath = file.getPath
			logger.info("file path = " + filePath)
			this.virtuosoClient.store(filePath)
			val errorCode = HttpURLConnection.HTTP_CREATED
			val status = "success, file uploaded to: " + filePath
			logger.info("file inserted.")
			val executionResult = new GeneralResult(null, null, filePath, null, null, status, errorCode, null)
			executionResult
		} catch {
			case e: Exception =>
				val errorMessage = "error processing uploaded file: " + e.getMessage
				logger.error(errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val status = "failed, error message = " + e.getMessage
				val executionResult = new GeneralResult(null, null, null, null, null, status, errorCode, null)
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

		val mappingContent:String = if(pMappingText == null) {
			val mappingFilePath = if(pMappingFilePath == null) {
				val mappingFilePathFromManifest = this.getR2RMLMappingDocumentFilePathFromManifestFile(manifestFilePath);
				mappingFilePathFromManifest;
			}  else {
				pMappingFilePath;
			}

			logger.info(s"reading r2rml file from $mappingFilePath ...");
			//val mappingFileContent = fromFile(mappingFilePath).getLines.mkString("\n");
			val mappingFileContent = scala.io.Source.fromURL(mappingFilePath).mkString; //DO NOT USE \n HERE!
			mappingFileContent;
		} else {
			pMappingText;
		}
		mappingContent;
	}

	def getAllTriplesMaps() : ListResult = {
		val prolog = "PREFIX rr: <http://www.w3.org/ns/r2rml#> \n"
		var queryString: String = prolog + "SELECT ?tm \n";
		queryString = queryString + " FROM <" + this.mappingpediaProperties.graphName + ">\n";
		queryString = queryString + " WHERE {?tm rr:logicalTable ?lt} \n";

		val m = VirtModel.openDatabaseModel(this.mappingpediaProperties.graphName, this.mappingpediaProperties.virtuosoJDBC
			, this.mappingpediaProperties.virtuosoUser, this.mappingpediaProperties.virtuosoPwd);

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








	def getSubclassesSummary(pClass:String) : ListResult = {
		//val classURI = MappingPediaUtility.getClassURI(pClass, "http://schema.org/");

		val normalizedClasses = MappingPediaUtility.normalizeTerm(pClass).distinct;
		logger.info(s"normalizedClasses = $normalizedClasses");

		val resultAux:List[String] = normalizedClasses.flatMap(normalizedClass => {
			logger.info(s"normalizedClass = $normalizedClass");
			val schemaClass:String = jenaClient.mapNormalizedTerms.getOrElse(normalizedClass, normalizedClass);
			logger.info(s"schemaClass = $schemaClass");
			jenaClient.getSubclassesSummary(schemaClass).results.asInstanceOf[List[String]]
		}).distinct;
		new ListResult(resultAux.size, resultAux)
	}

	def getSchemaOrgSubclassesDetail(aClass:String) : ListResult = {
		logger.info(s"jenaClient = $jenaClient")
		logger.info(s"this.ontologyModel = ${this.ontologyModel}")

		jenaClient.getSubclassesDetail(aClass);
	}






	def generateAdditionalTriples(mappingLanguage:String, manifestModel:Model, mappingDocumentModel:Model) : List[Triple] = {
		if("rml".equalsIgnoreCase(mappingLanguage)) {
			this.generateAdditionalTriplesForRML(manifestModel, mappingDocumentModel);
		} else {
			this.generateAdditionalTriplesForR2RML(manifestModel, mappingDocumentModel);
		}
	}

	def generateAdditionalTriplesForR2RML(manifestModel:Model, mappingDocumentModel:Model) : List[Triple] = {
		logger.info("generating additional triples for R2RML ...");

		var newTriples:List[Triple] = List.empty;

		val r2rmlMappingDocumentResources = manifestModel.listResourcesWithProperty(
			RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_MAPPINGDOCUMENT_CLASS);

		if(r2rmlMappingDocumentResources != null) {
			while(r2rmlMappingDocumentResources.hasNext()) {
				val r2rmlMappingDocumentResource = r2rmlMappingDocumentResources.nextResource();
				//logger.info("r2rmlMappingDocumentResource = " + r2rmlMappingDocumentResource);

				//improve this code using, get all x from ?x rr:LogicalTable ?lt
				//mapping documents do not always explicitly have a TriplesMap
				//val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
				//  				RDF.`type`, MappingPediaConstant.R2RML_TRIPLESMAP_CLASS);
				val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
					MappingPediaConstant.R2RML_LOGICALTABLE_PROPERTY);
				//logger.info("triplesMapResources = " + triplesMapResources);
				if(triplesMapResources != null) {
					while(triplesMapResources.hasNext()) {
						val triplesMapResource = triplesMapResources.nextResource();
						val newStatement = new StatementImpl(r2rmlMappingDocumentResource
							, MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMapResource);
						//logger.info("adding new hasTriplesMap statement: " + newStatement);
						val newTriple = newStatement.asTriple();
						newTriples = newTriples ::: List(newTriple);
					}
				}
			}
		}

		logger.info("newTriples = " + newTriples);
		newTriples;
	}

	def generateAdditionalTriplesForRML(manifestModel:Model, mappingDocumentModel:Model) : List[Triple] = {
		logger.info("generating additional triples for RML ...");

		val mappingDocumentResources = manifestModel.listResourcesWithProperty(
			RDF.`type`, MappingPediaConstant.MAPPINGPEDIAVOCAB_MAPPINGDOCUMENT_CLASS);

		val newTriples:List[Triple] = if(mappingDocumentResources != null) {
			mappingDocumentResources.toIterator.flatMap(mappingDocumentResource => {

				val triplesMapResources = mappingDocumentModel.listResourcesWithProperty(
					MappingPediaConstant.RML_LOGICALSOURCE_PROPERTY);


				if(triplesMapResources != null) {
					val newTriplesAux:List[Triple] = triplesMapResources.toIterator.map(triplesMapResource => {

						val newStatement = new StatementImpl(mappingDocumentResource
							, MappingPediaConstant.HAS_TRIPLES_MAPS_PROPERTY, triplesMapResource);
						val newTriple = newStatement.asTriple();
						newTriple
					}).toList;
					newTriplesAux
				} else {
					List.empty
				}
			}).toList;
		} else {
			List.empty
		}

		logger.info(s"newTriples = $newTriples");
		newTriples;
	}

	def storeManifestAndMapping(mappingLanguage:String, manifestFilePath:String, pMappingFilePath:String
															, clearGraphString:String, pReplaceMappingBaseURI:String, newMappingBaseURI:String
														 ): Unit = {

		val clearGraphBoolean = MappingPediaUtility.stringToBoolean(clearGraphString);
		//logger.info("clearGraphBoolean = " + clearGraphBoolean);

		val replaceMappingBaseURI = MappingPediaUtility.stringToBoolean(pReplaceMappingBaseURI);

		val manifestText = if(manifestFilePath != null ) {
			this.getManifestContent(manifestFilePath);
		} else {
			null;
		}

		val manifestModel = if(manifestText != null) {
			this.virtuosoClient.readModelFromString(manifestText, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);
		} else {
			null;
		}

		//mappingpediaEngine.manifestModel = manifestModel;

		val oldMappingText:String = this.getMappingContent(manifestFilePath, pMappingFilePath);

		val mappingText = if(replaceMappingBaseURI) {
			MappingPediaUtility.replaceBaseURI(oldMappingText.split("\n").toIterator
				, newMappingBaseURI).mkString("\n");
		} else {
			oldMappingText;
		}

		val mappingDocumentModel = this.virtuosoClient.readModelFromString(mappingText
			, MappingPediaConstant.MANIFEST_FILE_LANGUAGE);
		//mappingpediaEngine.mappingDocumentModel = mappingDocumentModel;

		//val virtuosoGraph = mappingpediaR2RML.getMappingpediaGraph();

		//val virtuosoGraph = MappingPediaUtility.getVirtuosoGraph(MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
			//, MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd, MappingPediaEngine.mappingpediaProperties.graphName);
		if(clearGraphBoolean) {
			try {
				this.virtuosoClient.virtGraph.clear();
			} catch {
				case e:Exception => {
					logger.error("unable to clear the graph: " + e.getMessage);
				}
			}
		}

		if(manifestModel != null) {
			logger.info("Storing manifest triples.");
			val manifestTriples = MappingPediaUtility.toTriples(manifestModel);
			//logger.info("manifestTriples = " + manifestTriples.mkString("\n"));
			this.virtuosoClient.store(manifestTriples, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);

			logger.info("Storing generated triples.");
			val additionalTriples = this.generateAdditionalTriples(mappingLanguage
				, manifestModel, mappingDocumentModel);
			logger.info("additionalTriples = " + additionalTriples.mkString("\n"));

			this.virtuosoClient.store(additionalTriples, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);
		}

		logger.info("Storing R2RML triples in Virtuoso.");
		val r2rmlTriples = MappingPediaUtility.toTriples(mappingDocumentModel);
		//logger.info("r2rmlTriples = " + r2rmlTriples.mkString("\n"));

		this.virtuosoClient.store(r2rmlTriples, true, MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS);


	}





	def addQueryFile(queryFileRef: MultipartFile, mappingpediaUsername:String, datasetID:String) : GeneralResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("datasetID = " + datasetID)

		try {
			val queryFile:File = MappingPediaUtility.multipartFileToFile(queryFileRef, datasetID)

			logger.info("storing a new query file in github ...")
			val commitMessage = "Add a new query file by mappingpedia-engine"
			val response = githubClient.encodeAndPutFile(mappingpediaUsername
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
				val executionResult = new GeneralResult(null, null, null
					, queryURL, null, responseStatusText, responseStatus, null)
				return executionResult
			}
			else {
				val executionResult = new GeneralResult(null, null, null
					, null , null, responseStatusText, responseStatus, null)
				return executionResult
			}
		} catch {
			case e: Exception =>
				val errorMessage = e.getMessage
				logger.error("error uploading a new query file: " + errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new GeneralResult(null, null, null
					, null, null, errorMessage, errorCode, null)
				return executionResult
		}
	}


	def getMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String):GeneralResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		val response = githubClient.getFile(
			//MappingPediaEngine.mappingpediaProperties.githubUser
			//MappingPediaEngine.mappingpediaProperties.githubAccessToken,
			mappingpediaUsername, mappingDirectory, mappingFilename)
		val responseStatus = response.getStatus
		logger.debug("responseStatus = " + responseStatus)
		val responseStatusText = response.getStatusText
		logger.debug("responseStatusText = " + responseStatusText)
		val executionResult = if (HttpURLConnection.HTTP_OK == responseStatus) {
			val githubMappingURL = response.getBody.getObject.getString("url")
			logger.debug("githubMappingURL = " + githubMappingURL)
			new GeneralResult(null, null, githubMappingURL, null, null, responseStatusText, responseStatus, null)
		} else {
			new GeneralResult(null, null, null, null, null, responseStatusText, responseStatus, null)
		}
		executionResult;
	}

	def updateExistingMapping(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
														, mappingFileRef:MultipartFile): GeneralResult = {
		logger.debug("mappingpediaUsername = " + mappingpediaUsername)
		logger.debug("mappingDirectory = " + mappingDirectory)
		logger.debug("mappingFilename = " + mappingFilename)
		logger.debug("mappingFileRef = " + mappingFileRef)
		try {
			val mappingFile = MappingPediaUtility.multipartFileToFile(mappingFileRef, mappingDirectory)
			val mappingFilePath = mappingFile.getPath
			logger.debug("mapping file path = " + mappingFilePath)
			val commitMessage = "Mapping modification by mappingpedia-engine.Application"
			val mappingContent = this.getMappingContent(null, null, mappingFilePath, null)
			val base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent)
			val response = githubClient.putEncodedContent(mappingpediaUsername, mappingDirectory, mappingFilename
				, commitMessage, base64EncodedContent)
			val responseStatus = response.getStatus
			logger.debug("responseStatus = " + responseStatus)
			val responseStatusText = response.getStatusText
			logger.debug("responseStatusText = " + responseStatusText)

			val executionResult = if (HttpURLConnection.HTTP_OK == responseStatus) {
				val githubMappingURL = response.getBody.getObject.getJSONObject("content").getString("url")
				logger.debug("githubMappingURL = " + githubMappingURL)
				new GeneralResult(null, null, githubMappingURL, null, null, responseStatusText, responseStatus, null)
			} else {
				new GeneralResult(null, null, null, null, null, responseStatusText, responseStatus, null)
			}
			executionResult;
		} catch {
			case e: Exception =>
				e.printStackTrace()
				val errorMessage = "error processing the uploaded mapping file: " + e.getMessage
				logger.error(errorMessage)
				val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
				val executionResult = new GeneralResult(null, null, null, null, null, errorMessage, errorCode, null)
				executionResult
		}
	}

	def setOntologyModel(ontModel: OntModel) = { this.ontologyModel = ontModel }

	def setProperties(properties: MappingPediaProperties) = { this.mappingpediaProperties = properties }




}
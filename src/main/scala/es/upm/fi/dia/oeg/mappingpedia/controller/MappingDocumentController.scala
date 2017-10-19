package es.upm.fi.dia.oeg.mappingpedia.controller

import java.net.HttpURLConnection
import java.util.{Date, UUID}

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.{Application, MappingPediaConstant, MappingPediaEngine, MappingPediaRunner}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.sdf
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddMappingDocumentResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility.{GitHubUtility, MappingPediaUtility}
import org.springframework.web.multipart.MultipartFile
import virtuoso.jena.driver.{VirtModel, VirtuosoQueryExecutionFactory}

object MappingDocumentController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def findMappingDocuments(queryString: String): ListResult = {
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);

    logger.info("Executing query=\n" + queryString)

    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    var results: List[MappingDocument] = List.empty;
    try {
      val rs = qexec.execSelect
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val id = MappingPediaUtility.getStringOrElse(qs, "md", null);
        val md = new MappingDocument(id);
        md.title = MappingPediaUtility.getStringOrElse(qs, "title", null);
        md.dataset = MappingPediaUtility.getStringOrElse(qs, "dataset", null);
        //md.filePath = MappingPediaUtility.getStringOrElse(qs, "mappingDocumentFile", null);
        md.creator = MappingPediaUtility.getStringOrElse(qs, "creator", null);
        md.distributionAccessURL = MappingPediaUtility.getStringOrElse(qs, "distributionAccessURL", null);
        md.mappingLanguage = MappingPediaUtility.getStringOrElse(qs, "mappingLanguage", null);
        md.dateSubmitted = MappingPediaUtility.getStringOrElse(qs, "dateSubmitted", null);
        md.accessURL = MappingPediaUtility.getStringOrElse(qs, "accessURL", null);

        results = md :: results;
      }
    } finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

  def findAllMappingDocuments(): ListResult = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappingDocuments.rq")

    MappingDocumentController.findMappingDocuments(queryString);

  }

  def findMappingDocumentsByMappedClass(mappedClass: String): ListResult = {
    logger.info("findMappingDocumentsByMappedClass:" + mappedClass)
    val queryTemplateFile = "templates/findTriplesMapsByMappedClass.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedClass" -> mappedClass
      //, "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByDatasetId(datasetId: String): ListResult = {
    logger.info("findMappingDocumentsByDatasetId:" + datasetId)
    val queryTemplateFile = "templates/findAllMappingDocumentsByDatasetId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$datasetId" -> datasetId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedProperty(mappedProperty: String): ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedProperty.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedColumn(mappedColumn: String): ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedColumn.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedColumn" -> mappedColumn
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedTable(mappedTable: String): ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedTable.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedTable" -> mappedTable
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    MappingDocumentController.findMappingDocuments(queryString);
  }

  def findMappingDocuments(searchType: String, searchTerm: String): ListResult = {
    val result: ListResult = if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_CLASS.equals(searchType) && searchTerm != null) {

      val listResult = MappingDocumentController.findMappingDocumentsByMappedClass(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_PROPERTY.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedProperty:" + searchTerm)
      val listResult = MappingDocumentController.findMappingDocumentsByMappedProperty(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_TABLE.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedTable:" + searchTerm)
      val listResult = MappingDocumentController.findMappingDocumentsByMappedTable(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_COLUMN.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedColumn:" + searchTerm)
      val listResult = MappingDocumentController.findMappingDocumentsByMappedColumn(searchTerm)
      listResult
    } else {
      logger.info("findAllMappingDocuments")
      val listResult = MappingDocumentController.findAllMappingDocuments
      listResult
    }
    logger.info("result = " + result)

    result;
  }

  def uploadNewMapping(dataset: Dataset, manifestFileRef: MultipartFile
                       , replaceMappingBaseURI: String, generateManifestFile: String
                       , mappingDocument: MappingDocument
                      ): AddMappingDocumentResult = {
    var errorOccured = false;
    var collectiveErrorMessage: List[String] = Nil;

    val organization = dataset.dctPublisher;

    logger.debug("organization.dctIdentifier = " + organization.dctIdentifier)
    logger.debug("dataset.dctIdentifier = " + dataset.dctIdentifier)

    val mappingFile = MappingPediaUtility.multipartFileToFile(mappingDocument.multipartFile, dataset.dctIdentifier)
    val mappingFilePath = mappingFile.getPath

    //STORING MAPPING FILE ON GITHUB
    val mappingFileGitHubResponse: HttpResponse[JsonNode] = try {
      val commitMessage = "add a new mapping file by mappingpedia-engine"
      val mappingContent = MappingPediaEngine.getMappingContent(mappingFilePath)
      val base64EncodedContent = GitHubUtility.encodeToBase64(mappingContent)
      logger.info("Storing mapping file on GitHub ...")
      val response = GitHubUtility.putEncodedContent(MappingPediaEngine.mappingpediaProperties.githubUser
        , MappingPediaEngine.mappingpediaProperties.githubAccessToken, organization.dctIdentifier, dataset.dctIdentifier, mappingFile.getName
        , commitMessage, base64EncodedContent)
      val responseStatus = response.getStatus
      if (HttpURLConnection.HTTP_OK == responseStatus
        || HttpURLConnection.HTTP_CREATED == responseStatus) {
        //val url = response.getBody.getObject.getJSONObject("content").getString("url")
        logger.info("Mapping stored on GitHub")
      } else {
        errorOccured = true;
        val errorMessage = "Error when storing mapping on GitHub: " + responseStatus
        logger.error("Error when storing mapping on GitHub: " + responseStatus)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
      }
      response
    } catch {
      case e: Exception =>
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error generating manifest file: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
    }
    val mappingDocumentGitHubURL = if (mappingFileGitHubResponse == null) {
      ""
    } else {
      mappingFileGitHubResponse.getBody.getObject.getJSONObject("content").getString("url")
    }


    //GENERATING MANIFEST FILE
    val manifestFile = try {
      if (manifestFileRef != null) {
        logger.info("Manifest file is provided")
        MappingPediaUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier)
      } else {
        logger.info("Manifest file is not provided")
        if (generateManifestFile != null && ("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile))) {
          //GENERATE MANIFEST FILE IF NOT PROVIDED
          logger.info("Generating manifest file ...")
          val templateFiles = List(
            MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA_NAMESPACE
            , MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA);

          val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

          val mapValues: Map[String, String] = Map(
            "$mappingDocumentID" -> mappingDocument.dctIdentifier
            , "$mappingDocumentTitle" -> mappingDocument.title
            , "$mappingDocumentDateTimeSubmitted" -> mappingDocumentDateTimeSubmitted
            , "$mappingDocumentCreator" -> mappingDocument.creator
            , "$mappingDocumentSubjects" -> mappingDocument.subject
            , "$mappingDocumentFilePath" -> mappingDocumentGitHubURL
            , "$datasetID" -> dataset.dctIdentifier
            , "$mappingLanguage" -> mappingDocument.mappingLanguage

            //, "$datasetTitle" -> datasetTitle
            //, "$datasetKeywords" -> datasetKeywords
            //, "$datasetPublisher" -> datasetPublisher
            //, "$datasetLanguage" -> datasetLanguage
          );

          val filename = "metadata-mappingdocument.ttl";
          val generatedManifestFile = MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, dataset.dctIdentifier);
          logger.info("Manifest file generated.")
          generatedManifestFile
        } else {
          null
        }
      }
    }
    catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace();
        val errorMessage = "Error occured when generating manifest file: " + e.getMessage;
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }

    //STORING MAPPING AND MANIFEST FILES ON VIRTUOSO
    val virtuosoStoreMappingStatus = try {
      logger.info("Storing mapping and manifest file on Virtuoso ...")
      val manifestFilePath: String = if (manifestFile == null) {
        null
      } else {
        manifestFile.getPath;
      }
      val newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS + dataset.dctIdentifier + "/"
      MappingPediaEngine.storeManifestAndMapping(manifestFilePath, mappingFilePath, "false"
        //, Application.mappingpediaEngine
        , replaceMappingBaseURI, newMappingBaseURI)
      logger.info("Mapping and manifest file stored on Virtuoso")
      "OK"
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace();
        val errorMessage = "Error occurred when storing mapping and manifest files on virtuoso: " + e.getMessage;
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        e.getMessage
      }
    }


    //STORING MANIFEST FILE ON GITHUB
    val addNewManifestResponse = try {
      if (manifestFile != null) {
        logger.info("Storing manifest file on GitHub ...")
        val addNewManifestCommitMessage = "Add a new manifest file by mappingpedia-engine"
        val githubResponse = GitHubUtility.putEncodedFile(MappingPediaEngine.mappingpediaProperties.githubUser
          , MappingPediaEngine.mappingpediaProperties.githubAccessToken, organization.dctIdentifier
          , dataset.dctIdentifier, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
        val addNewManifestResponseStatus = githubResponse.getStatus
        val addNewManifestResponseStatusText = githubResponse.getStatusText

        if (HttpURLConnection.HTTP_CREATED == addNewManifestResponseStatus
          || HttpURLConnection.HTTP_OK == addNewManifestResponseStatus) {
          logger.info("Manifest file stored on GitHub")
        } else {
          errorOccured = true;
          val errorMessage = "Error occured when storing manifest file on GitHub: " + addNewManifestResponseStatusText;
          logger.error(errorMessage)
          collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        }
        githubResponse
      } else {
        null
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace();
        val errorMessage = "Error occurred when storing manifest files on github: " + e.getMessage;
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    val manifestGitHubURL = if (addNewManifestResponse == null) {
      null
    } else {
      addNewManifestResponse.getBody.getObject.getJSONObject("content").getString("url")
    }

    val (responseStatus, responseStatusText) = if (errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }


    val addMappingResult:AddMappingDocumentResult = new AddMappingDocumentResult(
      responseStatus, responseStatusText
      , mappingDocumentGitHubURL
      , manifestGitHubURL

      , virtuosoStoreMappingStatus, virtuosoStoreMappingStatus
    )
    addMappingResult

    /*
    new MappingPediaExecutionResult(manifestGitHubURL, null, mappingDocumentGitHubURL
      , null, null, responseStatusText, responseStatus, null)
      */


  }
}

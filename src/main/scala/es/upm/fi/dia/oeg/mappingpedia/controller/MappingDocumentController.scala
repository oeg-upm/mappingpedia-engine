package es.upm.fi.dia.oeg.mappingpedia.controller

import java.net.HttpURLConnection
import java.util.Date

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.sdf
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddMappingDocumentResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility._
import org.springframework.web.multipart.MultipartFile

import scala.collection.JavaConversions._

class MappingDocumentController(val githubClient:GitHubUtility, val virtuosoClient: VirtuosoClient) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def storeMappingDocumentOnGitHub(mappingDocument:MappingDocument, dataset: Dataset) = {
    val organization = dataset.dctPublisher;

    val mappingDocumentDownloadURL = mappingDocument.getDownloadURL();

    val (mappingDocumentFileName:String, mappingDocumentFileContent:String) =
      MappingPediaUtility.getFileNameAndContent(mappingDocument.mappingDocumentFile, mappingDocumentDownloadURL, "UTF-8");
    val base64EncodedContent = GitHubUtility.encodeToBase64(mappingDocumentFileContent)

    val commitMessage = "add a new mapping file by mappingpedia-engine"
    //val mappingContent = MappingPediaEngine.getMappingContent(mappingFilePath)

    logger.info("Storing mapping file on GitHub ...")
    val response = githubClient.putEncodedContent(organization.dctIdentifier
      , dataset.dctIdentifier, mappingDocumentFileName
      , commitMessage, base64EncodedContent)
    val responseStatus = response.getStatus
    if (HttpURLConnection.HTTP_OK == responseStatus
      || HttpURLConnection.HTTP_CREATED == responseStatus) {
      val githubDownloadURL = response.getBody.getObject.getJSONObject("content").getString("download_url");
      mappingDocument.setDownloadURL(githubDownloadURL);
      logger.info("Mapping stored on GitHub")
    } else {
      val errorMessage = "Error when storing mapping on GitHub: " + responseStatus
      throw new Exception(errorMessage);
    }
    response
  }

  def addNewMappingDocument(dataset: Dataset, manifestFileRef: MultipartFile
                       , replaceMappingBaseURI: String, generateManifestFile: String
                       , mappingDocument: MappingDocument
                      ): AddMappingDocumentResult = {
    var errorOccured = false;
    var collectiveErrorMessage: List[String] = Nil;

    val organization = dataset.dctPublisher;


    val mappingDocumentFile = mappingDocument.mappingDocumentFile;
    //val mappingFilePath = mappingFile.getPath

    //STORING MAPPING DOCUMENT FILE ON GITHUB
    val mappingFileGitHubResponse: HttpResponse[JsonNode] = try {
      this.storeMappingDocumentOnGitHub(mappingDocument, dataset);
    } catch {
      case e: Exception =>
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error generating manifest file: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
    }
    mappingDocument.accessURL = this.githubClient.getAccessURL(mappingFileGitHubResponse)
    mappingDocument.setDownloadURL(this.githubClient.getDownloadURL(mappingDocument.accessURL))
    mappingDocument.sha = this.githubClient.getSHA(mappingDocument.accessURL);


    //MANIFEST FILE
    val manifestFile = try {
      if (manifestFileRef != null) {
        logger.info("Manifest file is provided")
        MappingPediaUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier)
      } else {
        logger.info("Manifest file is not provided")
        if ("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
          //GENERATE MANIFEST FILE IF NOT PROVIDED
          MappingDocumentController.generateManifestFile(mappingDocument, dataset);
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
    val virtuosoStoreMappingStatus = if(MappingPediaEngine.mappingpediaProperties.virtuosoEnabled) {
      try {
        logger.info("Storing mapping and manifest file on Virtuoso ...")
        val manifestFilePath: String = if (manifestFile == null) {
          null
        } else {
          manifestFile.getPath;
        }
        val newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS + dataset.dctIdentifier + "/"
        MappingPediaEngine.storeManifestAndMapping(manifestFilePath, mappingDocument.getDownloadURL(), "false"
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
    } else {
      "Storing to Virtuoso is not enabled!";
    }



    //STORING MANIFEST FILE ON GITHUB
    val addNewManifestResponse = try {
      if (manifestFile != null) {
        logger.info("Storing manifest file on GitHub ...")
        val addNewManifestCommitMessage = s"Add a new manifest file for mapping document: ${mappingDocument.dctIdentifier}"
        val githubResponse = githubClient.encodeAndPutFile(organization.dctIdentifier
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
    mappingDocument.manifestAccessURL = if (addNewManifestResponse == null) {
      null
    } else {
      addNewManifestResponse.getBody.getObject.getJSONObject("content").getString("url")
    }
    mappingDocument.manifestDownloadURL = this.githubClient.getDownloadURL(mappingDocument.manifestAccessURL);

    val (responseStatus, responseStatusText) = if (errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }


    val addMappingResult:AddMappingDocumentResult = new AddMappingDocumentResult(
      responseStatus, responseStatusText
      //, mappingDocumentAccessURL, mappingDocumentDownloadURL, mappingDocument.sha
      , mappingDocument
      //, manifestAccessURL, manifestDownloadURL
      , virtuosoStoreMappingStatus, virtuosoStoreMappingStatus
    )
    addMappingResult

    /*
    new MappingPediaExecutionResult(manifestGitHubURL, null, mappingDocumentGitHubURL
      , null, null, responseStatusText, responseStatus, null)
      */


  }



  def findAllMappedClasses(): ListResult = {
    this.findAllMappedClasses("http://schema.org")
  }

  def findAllMappedClasses(prefix:String): ListResult = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$prefix" -> prefix
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappedClasses.rq")

    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = this.virtuosoClient.createQueryExecution(queryString);

    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedClass = qs.get("mappedClass").toString;
        results = mappedClass :: results;
      }
    } finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

  def findAllMappedClassesByTableName(tableName:String): ListResult = {
    this.findAllMappedClassesByTableName("http://schema.org", tableName)
  }

  def findAllMappedClassesByTableName(prefix:String, tableName:String): ListResult = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$tableName" -> tableName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappedClassesByMappedTable.rq")

    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = this.virtuosoClient.createQueryExecution(queryString);
  logger.info(s"queryString = \n$queryString")

    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedClass = qs.get("mappedClass").toString;
        val count = qs.get("count").toString;
        logger.info(s"mappedClass = $mappedClass")
        logger.info(s"count = $count")

        results = s"$mappedClass -- $count" :: results;
      }
    }
      catch {
        case e:Exception => { e.printStackTrace()}
      }
    finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

  def findAllMappedProperties(prefix:String): ListResult = {
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$prefix" -> prefix
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappedProperties.rq")

    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = this.virtuosoClient.createQueryExecution(queryString);


    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedProperty = qs.get("mappedProperty").toString;
        results = mappedProperty :: results;
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

    this.findMappingDocuments(queryString);

  }

  def findMappingDocuments(searchType: String, searchTerm: String): ListResult = {
    val result: ListResult = if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_CLASS.equals(searchType) && searchTerm != null) {

      val listResult = this.findMappingDocumentsByMappedClass(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_PROPERTY.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedProperty:" + searchTerm)
      val listResult = this.findMappingDocumentsByMappedProperty(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_TABLE.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedTable:" + searchTerm)
      val listResult = this.findMappingDocumentsByMappedTable(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_COLUMN.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedColumn:" + searchTerm)
      val listResult = this.findMappingDocumentsByMappedColumn(searchTerm)
      listResult
    } else {
      logger.info("findAllMappingDocuments")
      val listResult = this.findAllMappingDocuments
      listResult
    }
    //logger.info("result = " + result)

    result;
  }

  def findMappingDocumentsByDatasetId(datasetId: String): ListResult = {
    logger.info("findMappingDocumentsByDatasetId:" + datasetId)
    val queryTemplateFile = "templates/findAllMappingDocumentsByDatasetId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$datasetId" -> datasetId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedClass(mappedClass: String): ListResult = {
    //logger.info("findMappingDocumentsByMappedClass:" + mappedClass)
    val queryTemplateFile = "templates/findTriplesMapsByMappedClass.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedClass" -> mappedClass
      //, "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedSubClass(aClass: String, jenaClient: JenaClient): ListResult = {
    val classURI = MappingPediaUtility.getClassURI(aClass);

    val normalizedClassURI = jenaClient.mapNormalizedTerms.getOrElse(classURI, classURI);

    val subclassesURIs:List[String] = jenaClient.getSubclassesSummary(normalizedClassURI).results.asInstanceOf[List[String]];

    val allMappedClasses:List[String] = this.findAllMappedClasses().results.asInstanceOf[List[String]]

    val intersectedClasses = subclassesURIs.intersect(allMappedClasses);
    
    val mappingDocuments = intersectedClasses.flatMap(intersectedClass => {
      this.findMappingDocumentsByMappedClass(intersectedClass).getResults();
    }).asInstanceOf[Iterable[MappingDocument]];

    val listResult = new ListResult(mappingDocuments.size, mappingDocuments)
    listResult
  }

  def findMappingDocumentsByDistributionId(distributionId: String): ListResult = {
    logger.info("findMappingDocumentsByDistributionId:" + distributionId)
    val queryTemplateFile = "templates/findAllMappingDocumentsByDistributionId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$distributionId" -> distributionId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappingDocumentId(mappingDocumentId: String): MappingDocument = {
    logger.info("findMappingDocumentsByMappingDocumentId:" + mappingDocumentId)
    val queryTemplateFile = "templates/findAllMappingDocumentsByMappingDocumentId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappingDocumentId" -> mappingDocumentId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    val resultAux = this.findMappingDocuments(queryString).getResults();
    val result = if(resultAux != null) {
      resultAux.iterator().next().asInstanceOf[MappingDocument]
    } else {
      null
    }
    result

  }

  def findMappingDocumentsByMappedProperty(mappedProperty: String): ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedProperty.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedColumn(mappedColumn: String): ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedColumn.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedColumn" -> mappedColumn
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findMappingDocuments(queryString);
  }

  def findMappingDocumentsByMappedTable(mappedTable: String): ListResult = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedTable.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedTable" -> mappedTable
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findMappingDocuments(queryString);
  }

  def findMappingDocuments(queryString: String): ListResult = {
    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = this.virtuosoClient.createQueryExecution(queryString);
    logger.info(s"queryString = $queryString")

    var results: List[MappingDocument] = List.empty;
    try {
      var retrievedMappings:List[String] = Nil;

      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {

        val qs = rs.nextSolution
        val mdID= qs.get("mdID").toString;
        val md = new MappingDocument(mdID);
        md.dctTitle = MappingPediaUtility.getStringOrElse(qs, "title", null);
        val datasetId = MappingPediaUtility.getStringOrElse(qs, "datasetId", null);
        md.dataset = new Dataset(datasetId)
        md.dataset.dctTitle = MappingPediaUtility.getStringOrElse(qs, "datasetTitle", null);
        val distribution = new Distribution(md.dataset);
        distribution.dcatAccessURL= MappingPediaUtility.getStringOrElse(qs, "distributionAccessURL", null);
        distribution.dcatDownloadURL= MappingPediaUtility.getStringOrElse(qs, "distributionDownloadURL", null);
        distribution.sha = MappingPediaUtility.getStringOrElse(qs, "distributionSHA", null);

        //md.dataset = MappingPediaUtility.getStringOrElse(qs, "dataset", null);
        //md.filePath = MappingPediaUtility.getStringOrElse(qs, "mappingDocumentFile", null);
        md.dctCreator = MappingPediaUtility.getStringOrElse(qs, "creator", null);

        md.mappingLanguage = MappingPediaUtility.getStringOrElse(qs, "mappingLanguage", null);
        md.dctDateSubmitted = MappingPediaUtility.getStringOrElse(qs, "dateSubmitted", null);
        md.sha = MappingPediaUtility.getStringOrElse(qs, "mdSHA", null);
        val mdDownloadURL = MappingPediaUtility.getStringOrElse(qs, "mdDownloadURL", null);
        md.setDownloadURL(mdDownloadURL);
        //logger.info(s"md.distributionSHA = ${md.distributionSHA}");
        //logger.info(s"md.sha = ${md.sha}");

        if(!retrievedMappings.contains(md.sha)) {
          results = md :: results;
          retrievedMappings = md.sha :: retrievedMappings
        }

      }
    } finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

}

object MappingDocumentController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  //val githubClient = MappingPediaEngine.githubClient;



















  def generateManifestFile(mappingDocument: MappingDocument, dataset: Dataset) = {
    logger.info("Generating mapping document manifest file ...")
    val templateFiles = List(
      MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA_NAMESPACE
      , MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA);

    val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

    val mapValues: Map[String, String] = Map(
      "$mappingDocumentID" -> mappingDocument.dctIdentifier
      , "$mappingDocumentTitle" -> mappingDocument.dctTitle
      , "$mappingDocumentDateTimeSubmitted" -> mappingDocumentDateTimeSubmitted
      , "$mappingDocumentCreator" -> mappingDocument.dctCreator
      , "$mappingDocumentSubjects" -> mappingDocument.dctSubject
      , "$mappingDocumentFilePath" -> mappingDocument.getDownloadURL()
      , "$datasetID" -> dataset.dctIdentifier
      , "$mappingLanguage" -> mappingDocument.mappingLanguage
      , "$sha" -> mappingDocument.sha

      //, "$datasetTitle" -> datasetTitle
      //, "$datasetKeywords" -> datasetKeywords
      //, "$datasetPublisher" -> datasetPublisher
      //, "$datasetLanguage" -> datasetLanguage
    );

    val filename = "metadata-mappingdocument.ttl";
    val generatedManifestFile = MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, dataset.dctIdentifier);
    logger.info("Manifest file generated.")
    generatedManifestFile
  }


  def detectMappingLanguage(pMappingLanguage:String) : String = {
    val mappingLanguage = if (pMappingLanguage != null) {
      val splitedMappingLanguage = pMappingLanguage.split(".")
      if (splitedMappingLanguage.length == 3 && "rml".equalsIgnoreCase(splitedMappingLanguage(1)) && ".ttl".equalsIgnoreCase(splitedMappingLanguage(2))) {
        "rml"
      } else {
        "r2rml"
      }
    } else {
      "r2rml"
    }
    mappingLanguage
  }
}

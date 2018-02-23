package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddMappingDocumentResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility._
import org.springframework.web.multipart.MultipartFile

import scala.collection.JavaConversions._

class MappingDocumentController(val ckanClient:CKANUtility, val githubClient:GitHubUtility, val virtuosoClient: VirtuosoClient, val jenaClient:JenaClient) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val mapper = new ObjectMapper();
  val datasetController = new DatasetController(ckanClient, githubClient, virtuosoClient);

  def findOrCreate(id:String): MappingDocument = {
    val existingMappingDocument = if(id != null) {this.findById(id);} else { null }
    val mappingDocument = if(existingMappingDocument == null) {
      new MappingDocument();
    } else { existingMappingDocument }
    mappingDocument
  }

  def storeMappingDocumentOnGitHub(mappingDocument:MappingDocument, dataset: Dataset) = {
    val organization = dataset.dctPublisher;

    val mappingDocumentDownloadURL = mappingDocument.getDownloadURL();

    val (mappingDocumentFileName:String, mappingDocumentFileContent:String) =
      MappingPediaUtility.getFileNameAndContent(mappingDocument.mappingDocumentFile, mappingDocumentDownloadURL, "UTF-8");
    val base64EncodedContent = GitHubUtility.encodeToBase64(mappingDocumentFileContent)

    //val mappingDocumentFilePath = s"${organization.dctIdentifier}/${dataset.dctIdentifier}/${mappingDocument.dctIdentifier}/${mappingDocumentFileName}";
    val mappingDocumentFilePath = s"${organization.dctIdentifier}/${dataset.dctIdentifier}/${mappingDocumentFileName}";
    logger.info(s"mappingDocumentFilePath = $mappingDocumentFilePath")

    val commitMessage = s"add mapping document file ${mappingDocumentFileName}"
    //val mappingContent = MappingPediaEngine.getMappingContent(mappingFilePath)

    logger.info("STORING MAPPING DOCUMENT FILE ON GITHUB ...")
    val response = githubClient.putEncodedContent(
      mappingDocumentFilePath, commitMessage, base64EncodedContent)
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
                            , replaceMappingBaseURI: String, generateManifestFile: Boolean
                            , mappingDocument: MappingDocument
                           ): AddMappingDocumentResult = {
    var errorOccured = false;
    var collectiveErrorMessage: List[String] = Nil;

    val organization = dataset.dctPublisher;
    val ckanPackageId = if(dataset.ckanPackageId == null) {
      try {
        val foundDataset = this.datasetController.findByCKANPackageName(dataset.dctIdentifier);
        if(foundDataset != null) {
          foundDataset.results.iterator.next.ckanPackageId
        } else { null }
      } catch {
        case e:Exception => null
      }
    } else { dataset.ckanPackageId }
    logger.info("ckanPackageId = " + ckanPackageId);
    dataset.ckanPackageId = ckanPackageId;


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
    val mappingDocumentDownloadURL = mappingDocument.getDownloadURL();
    mappingDocument.hash = MappingPediaUtility.calculateHash(mappingDocumentDownloadURL).toString;


    //MANIFEST FILE
    val manifestFile = try {
      if (manifestFileRef != null) {
        logger.info("Manifest file is provided")
        MappingPediaUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier)
      } else {
        logger.info("Manifest file is not provided")
        if (generateManifestFile) {
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
        logger.info("STORING MAPPING DOCUMENT AND ITS MANIFEST ON VIRTUOSO ...")
        val manifestFilePath: String = if (manifestFile == null) {
          null
        } else {
          manifestFile.getPath;
        }
        val newMappingBaseURI = MappingPediaConstant.MAPPINGPEDIA_INSTANCE_NS + mappingDocument.dctIdentifier + "/"
        val clearGraph = "false";
        MappingPediaEngine.storeManifestAndMapping(mappingDocument.mappingLanguage
          , manifestFilePath, mappingDocument.getDownloadURL(), clearGraph
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
        logger.info("STORING MANIFEST FILE ON GITHUB ...")
        val addNewManifestCommitMessage = s"Add mapping document manifest: ${mappingDocument.dctIdentifier}"

        val mappingDocumentManifestFilePath = s"${organization.dctIdentifier}/${dataset.dctIdentifier}/${mappingDocument.dctIdentifier}/${manifestFile.getName}";
        logger.info(s"mappingDocumentManifestFilePath = $mappingDocumentManifestFilePath")

        val githubResponse = githubClient.encodeAndPutFile(
          mappingDocumentManifestFilePath, addNewManifestCommitMessage, manifestFile)
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

    try {
      val addMappingDocumentResultAsJson = this.mapper.writeValueAsString(addMappingResult);
      logger.info(s"addMappingDocumentResultAsJson = ${addMappingDocumentResultAsJson}\n\n");
    } catch {
      case e:Exception => {
        logger.error(s"addMappingResult = ${addMappingResult}")
      }
    }

    addMappingResult

    /*
    new MappingPediaExecutionResult(manifestGitHubURL, null, mappingDocumentGitHubURL
      , null, null, responseStatusText, responseStatus, null)
      */


  }



  def findAllMappedClasses(): ListResult[String] = {
    this.findAllMappedClasses("http://schema.org")
  }

  def findAllMappedClasses(prefix:String): ListResult[String] = {

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

    val listResult = new ListResult[String](results.length, results);
    listResult
  }

  def findMappedClassesByMappingDocumentId(mappingDocumentId:String) = {
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$mappingDocumentId" -> mappingDocumentId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findMappedClassesByMappingDocumentId.rq")

    val qexec = this.virtuosoClient.createQueryExecution(queryString);
    logger.debug(s"queryString = \n$queryString")

    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedClass = qs.get("mappedClass").toString;
        logger.info(s"mappedClass = $mappedClass")

        results = mappedClass :: results;
      }
    }
    catch {
      case e:Exception => { e.printStackTrace()}
    }
    finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

  def findAllMappedClassesByTableName(tableName:String): ListResult[String] = {
    this.findAllMappedClassesByTableName("http://schema.org", tableName)
  }

  def findAllMappedClassesByTableName(prefix:String, tableName:String): ListResult[String] = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName,
      "$tableName" -> tableName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findMappedClassesByMappedTable.rq")

    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = this.virtuosoClient.createQueryExecution(queryString);
    logger.debug(s"queryString = \n$queryString")

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

    val listResult = new ListResult[String](results.length, results);
    listResult
  }

  def findAllMappedProperties(prefix:String) = {
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


  def findAll() = {

    //val queryString: String = MappingPediaUtility.readFromResourcesDirectory("templates/findAllMappingDocuments.rq")
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findAllMappingDocuments.rq")

    this.findByQueryString(queryString);

  }

  def findByTerm(searchType: String, searchTerm: String): ListResult[MappingDocument] = {
    val result = if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_CLASS.equals(searchType) && searchTerm != null) {

      val listResult = this.findByClass(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_PROPERTY.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedProperty:" + searchTerm)
      val listResult = this.findByProperty(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_TABLE.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedTable:" + searchTerm)
      val listResult = this.findByTable(searchTerm)
      listResult
    } else if (MappingPediaConstant.SEARCH_MAPPINGDOCUMENT_BY_COLUMN.equals(searchType) && searchTerm != null) {
      logger.info("findMappingDocumentsByMappedColumn:" + searchTerm)
      val listResult = this.findByColumn(searchTerm)
      listResult
    } else {
      logger.info("findAllMappingDocuments")
      val listResult = this.findAll
      listResult
    }
    //logger.info("result = " + result)

    result;
  }

  def findByDatasetId(pDatasetId: String, pCKANPackageId:String
                                      , pCKANPackageName:String) = {

    val queryTemplateFile = "templates/findMappingDocumentsByDatasetId.rq";

    val datasetId = if(pDatasetId != null) {
      pDatasetId
    } else { "" }

    val ckanPackageId = if(pCKANPackageId != null) {
      pCKANPackageId
    } else { "" }

    val ckanPackageName = if(pCKANPackageName != null) {
      pCKANPackageName
    } else { "" }

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$datasetId" -> datasetId
      , "$ckanPackageName" -> ckanPackageId
      , "$ckanPackageId" -> ckanPackageName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    logger.debug(s"queryString = ${queryString}")
    this.findByQueryString(queryString);
  }

  def findByClass(mappedClass: String) = {
    //logger.info("findMappingDocumentsByMappedClass:" + mappedClass)
    val queryTemplateFile = "templates/findTriplesMapsByMappedClass.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedClass" -> mappedClass
      //, "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByClassAndProperty(mappedClass: String, mappedProperty:String): ListResult[MappingDocument] = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedClassAndProperty.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedClass" -> mappedClass
      , "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByClassAndProperty(aClass: String, aProperty:String, subclass: Boolean): ListResult[MappingDocument] = {
    val classURI = MappingPediaUtility.getClassURI(aClass);

    val normalizedClassURI = this.jenaClient.mapNormalizedTerms.getOrElse(classURI, classURI);

    //val subclassesURIs:List[String] = jenaClient.getSubclassesSummary(normalizedClassURI).results.asInstanceOf[List[String]];
    val subclassesURIs:List[String] = if(subclass) {
      jenaClient.getSubclassesSummary(normalizedClassURI).results.toList;
    } else {
      List(normalizedClassURI)
    }

    val allMappedClasses:List[String] = this.findAllMappedClasses().results.toList

    val intersectedClasses = subclassesURIs.intersect(allMappedClasses);

    val mappingDocuments = intersectedClasses.flatMap(intersectedClass => {
      this.findByClassAndProperty(intersectedClass, aProperty).getResults();
    });

    val listResult = new ListResult[MappingDocument](mappingDocuments.size, mappingDocuments)
    listResult
  }

  def findByDistributionId(distributionId: String) = {
    logger.info("findMappingDocumentsByDistributionId:" + distributionId)
    val queryTemplateFile = "templates/findMappingDocumentsByDistributionId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$distributionId" -> distributionId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findById(mappingDocumentId: String): MappingDocument = {
    logger.info("findMappingDocumentsByMappingDocumentId:" + mappingDocumentId)
    val queryTemplateFile = "templates/findMappingDocumentsByMappingDocumentId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappingDocumentId" -> mappingDocumentId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    logger.debug(s"queryString = ${queryString}")

    val resultAux = this.findByQueryString(queryString).getResults();
    val result = if(resultAux != null && resultAux.iterator.size > 0) {
      resultAux.iterator().next()
    } else {
      null
    }
    result
  }

  def findByProperty(mappedProperty: String) = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedProperty.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedProperty" -> mappedProperty
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByColumn(mappedColumn: String) = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedColumn.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedColumn" -> mappedColumn
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByTable(mappedTable: String) = {
    val queryTemplateFile = "templates/findTriplesMapsByMappedTable.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mappedTable" -> mappedTable
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findByQueryString(queryString);
  }

  def findByQueryString(queryString: String): ListResult[MappingDocument] = {
    //logger.info(s"queryString = $queryString");
    /*
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    */
    val qexec = this.virtuosoClient.createQueryExecution(queryString);
    //logger.info(s"queryString = \n$queryString")

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
        md.dataset.dctModified = MappingPediaUtility.getStringOrElse(qs, "datasetModified", null);
        md.dataset.dctTitle = MappingPediaUtility.getStringOrElse(qs, "datasetTitle", null);

        val distribution = new UnannotatedDistribution(md.dataset);
        distribution.dcatAccessURL= MappingPediaUtility.getStringOrElse(qs, "distributionAccessURL", null);

        distribution.dcatDownloadURL= MappingPediaUtility.getStringOrElse(qs, "distributionDownloadURL", null);

        distribution.hash = MappingPediaUtility.getStringOrElse(qs, "distributionHash", null);


        //md.dataset = MappingPediaUtility.getStringOrElse(qs, "dataset", null);
        //md.filePath = MappingPediaUtility.getStringOrElse(qs, "mappingDocumentFile", null);
        md.dctCreator = MappingPediaUtility.getStringOrElse(qs, "creator", null);

        md.mappingLanguage = MappingPediaUtility.getStringOrElse(qs, "mappingLanguage", null);

        md.dctDateSubmitted = MappingPediaUtility.getStringOrElse(qs, "dateSubmitted", null);

        md.hash = MappingPediaUtility.getStringOrElse(qs, "mdHash", null);

        md.ckanPackageId = MappingPediaUtility.getStringOrElse(qs, "packageId", null);

        md.ckanResourceId = MappingPediaUtility.getStringOrElse(qs, "resourceId", null);

        md.setDownloadURL(MappingPediaUtility.getStringOrElse(qs, "mdDownloadURL", null));
        //logger.info(s"md.sha = ${md.sha}");

        md.isOutdated = MappingDocumentController.isOutdatedMappingDocument(
          md.dctDateSubmitted, md.dataset.dctModified);

        if(!retrievedMappings.contains(md.hash)) {
          //logger.warn(s"retrieving mapping document with sha ${md.sha}.")
          //results = md :: results;
          results = results :+ md

          retrievedMappings = retrievedMappings :+ md.hash
        } else {
          //logger.warn(s"mapping document with sha ${md.sha} has been retrived.")
        }

      }
    } finally qexec.close

    logger.debug(s"results.length = ${results.length}")

    val listResult = new ListResult[MappingDocument](results.length, results);
    listResult
  }

}

object MappingDocumentController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def generateManifestFile(mappingDocument: MappingDocument, dataset: Dataset) = {
    logger.info("GENERATING MANIFEST FILE FOR MAPPING DOCUMENT ...")
    logger.info(s"mappingDocument.hash = ${mappingDocument.hash}")


    val templateFiles = List(
      MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA_NAMESPACE
      , MappingPediaConstant.TEMPLATE_MAPPINGDOCUMENT_METADATA);

    val mappingDocumentDateTimeSubmitted = MappingPediaEngine.sdf.format(new Date())

    val mapValues: Map[String, String] = Map(
      "$mappingDocumentID" -> mappingDocument.dctIdentifier
      , "$mappingDocumentTitle" -> mappingDocument.dctTitle
      , "$mappingDocumentDateTimeSubmitted" -> mappingDocumentDateTimeSubmitted
      , "$mappingDocumentCreator" -> mappingDocument.dctCreator
      , "$mappingDocumentSubjects" -> mappingDocument.dctSubject
      , "$mappingDocumentFilePath" -> mappingDocument.getDownloadURL()
      , "$datasetID" -> dataset.dctIdentifier
      , "$mappingLanguage" -> mappingDocument.mappingLanguage
      , "$hash" -> mappingDocument.hash

      , "$ckanPackageID" -> dataset.ckanPackageId
      , "$ckanResourceID" -> mappingDocument.ckanResourceId

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


  def detectMappingLanguage(mappingDocumentFile:File) : String = {
    if(mappingDocumentFile == null) {
      "r2rml"
    } else {
      val absolutePath = mappingDocumentFile.getAbsolutePath;
      this.detectMappingLanguage(absolutePath);
    }
  }

  def detectMappingLanguage(mappingDocumentDownloadURL:String) : String = {

    val mappingLanguage = if (mappingDocumentDownloadURL != null) {
      val splitedMappingLanguage = mappingDocumentDownloadURL.split("\\.")

      if (splitedMappingLanguage.length >= 3) {
        val extension1 = splitedMappingLanguage(splitedMappingLanguage.length-2);
        val extension2 = splitedMappingLanguage(splitedMappingLanguage.length-1);

        if("rml".equalsIgnoreCase(extension1) && "ttl".equalsIgnoreCase(extension2)) {
          "rml"
        } else {
          "r2rml"
        }
      } else {
        "r2rml"
      }
    } else {
      "r2rml"
    }
    mappingLanguage
  }

  def isOutdatedMappingDocument(pMdSubmitted:String, pDatasetModified:String) = {
    val mdSubmitted:Date = if(pMdSubmitted != null) {MappingPediaEngine.sdf.parse(pMdSubmitted); } else { null }
    val dsModified:Date = if(pDatasetModified != null) { MappingPediaEngine.sdf.parse(pDatasetModified); } else { null}

    val isOutdated = if(dsModified == null || mdSubmitted == null) { false }
    else {
      if( dsModified.after(mdSubmitted)) {
        true
      } else {
        false
      }
    }

    isOutdated
  }
}

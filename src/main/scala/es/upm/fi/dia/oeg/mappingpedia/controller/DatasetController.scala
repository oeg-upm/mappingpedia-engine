package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddDatasetResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANUtility, GitHubUtility, MappingPediaUtility, VirtuosoClient}
import org.springframework.web.multipart.MultipartFile

class DatasetController(val ckanClient:CKANUtility, val githubClient:GitHubUtility, val virtuosoClient: VirtuosoClient)  {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val distributionController = new DistributionController(ckanClient, githubClient, virtuosoClient: VirtuosoClient);
  val mapper = new ObjectMapper();

  def addModifiedDate(dataset: Dataset) = {

    val now = MappingPediaEngine.sdf.format(new Date())
    dataset.dctModified = now;

    try {
      val mapValues: Map[String, String] = Map(
        "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
        , "$datasetID" -> dataset.dctIdentifier
        , "$datasetModified" -> dataset.dctModified
      );

      val templateFiles = List("templates/metadata-namespaces-template.ttl"
        , "templates/addDatasetModifiedDate.ttl");
      val triplesString: String =MappingPediaEngine.generateStringFromTemplateFiles(
          mapValues, templateFiles)
      logger.info(s"adding triples to virtuoso: ${triplesString}");

      if(triplesString != null) {
        this.virtuosoClient.storeFromString(triplesString);
      }
    } catch {
      case e:Exception => {
        e.printStackTrace()
      }
    }
  }

  def findAllDatasets() = {
    logger.info("findDatasets")
    val queryTemplateFile = "templates/findAllDatasets.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findDatasets(queryString);
  }

  def findDatasetsByCKANPackageId(ckanPackageId:String) = {
    logger.info("findDatasetsByCKANPackageId")
    val queryTemplateFile = "templates/findDatasetByCKANPackageId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$ckanPackageId" -> ckanPackageId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findDatasets(queryString);
  }

  def findDatasetsByCKANPackageName(ckanPackageName:String) = {
    logger.info("findDatasetsByCKANPackageName")
    val queryTemplateFile = "templates/findDatasetByCKANPackageName.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$ckanPackageName" -> ckanPackageName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findDatasets(queryString);
  }

  def findDatasets(queryString: String): ListResult[Dataset] = {
    logger.debug(s"queryString = $queryString");

    /*    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
          , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);
        val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)*/

    val qexec = this.virtuosoClient.createQueryExecution(queryString);

    var results: List[Dataset] = List.empty;
    try {
      val rs = qexec.execSelect
      //logger.info("Obtaining result from executing query=\n" + queryString)
      while (rs.hasNext) {

        val qs = rs.nextSolution
        val datasetID = qs.get("datasetID").toString;
        val dataset = new Dataset(datasetID);
        dataset.dctTitle = MappingPediaUtility.getStringOrElse(qs, "datasetTitle", null)
        dataset.ckanPackageId = MappingPediaUtility.getStringOrElse(qs, "ckanPackageId", null)
        dataset.ckanPackageName = MappingPediaUtility.getStringOrElse(qs, "ckanPackageName", null)

        val distributionID = MappingPediaUtility.getStringOrElse(qs, "distributionID", null)
        val distribution = new UnannotatedDistribution(dataset, distributionID);
        distribution.dcatAccessURL = MappingPediaUtility.getStringOrElse(qs, "distributionAccessURL", null)
        distribution.dcatDownloadURL = MappingPediaUtility.getStringOrElse(qs, "distributionDownloadURL", null)

        val mdID = MappingPediaUtility.getStringOrElse(qs, "mdID", null)
        val md = new MappingDocument(mdID);
        md.setDownloadURL(MappingPediaUtility.getStringOrElse(qs, "mdDownloadURL", null))

        results = dataset :: results;
      }
    }
    catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s"Error execution query: ${e.getMessage}")
      }
    }
    finally qexec.close

    val listResult = new ListResult[Dataset](results.length, results);
    listResult
  }

  def storeManifestFileOnGitHub(file:File, dataset:Dataset) = {
    val organization = dataset.dctPublisher;
    val datasetId = dataset.dctIdentifier;
    val manifestFileName = file.getName
    val organizationId = organization.dctIdentifier;

    val addNewManifestCommitMessage = s"Add dataset manifest file: $datasetId"

    logger.info(s"STORING MANIFEST FILE FOR DATASET $datasetId ON GITHUB ... ")
    val githubResponse = githubClient.encodeAndPutFile(organization.dctIdentifier
      , datasetId, manifestFileName, addNewManifestCommitMessage, file)
    logger.info(s"manifest file for the dataset $datasetId stored on github.")
    githubResponse
  }


  def addDataset(dataset:Dataset, manifestFileRef:MultipartFile
                 , generateManifestFile:Boolean, storeToCKAN:Boolean
                ) : AddDatasetResult = {
    val distributions = dataset.dcatDistributions;

    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;

    //STORING DATASET AS PACKAGE ON CKAN
    val ckanAddPackageResponse:HttpResponse[JsonNode] = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable && storeToCKAN) {
        logger.info("STORING DATASET AS A PACKAGE ON CKAN ...")
        val response = ckanClient.addNewPackage(dataset);
        response
      } else {
        null
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing the dataset as a package on CKAN: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }


    val ckanAddPackageResponseResult = if(ckanAddPackageResponse != null) {
      try {
        ckanAddPackageResponse.getBody.getObject.getJSONObject("result");
      } catch {
        case e:Exception => {
          logger.error(s"error obtaining ckanAddPackageResponseResult: ${e.getMessage}")
          null
        }
      }
    } else {
      null
    }



    if(ckanAddPackageResponseResult != null) {
      dataset.ckanPackageId = ckanAddPackageResponseResult.getString("id");
      dataset.ckanPackageName = ckanAddPackageResponseResult.getString("name");
    }

    logger.info(s"dataset.ckanPackageId = ${dataset.ckanPackageId}");
    logger.info(s"dataset.ckanPackageName = ${dataset.ckanPackageName}");




    /*
    //STORING DISTRIBUTION FILE AS RESOURCE ON CKAN
    val ckanAddResourceResponse = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable) {
        logger.info("storing distribution file as a package on CKAN ...")

        if(distribution != null) {
          ckanClient.createResource(distribution);
        } else {
          null
        }
      } else {
        null
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing distribution file as a resource on CKAN: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    */

    if(distributions != null) {
      distributions.map(distribution => {
        //CALLING ADD DISTRIBUTION IN DISTRIBUTIONCONTROLLER
        logger.info(s"distribution = " + distribution);
        val addDistributionResult = if(distribution != null) {
          this.distributionController.addDistribution(distribution, manifestFileRef:MultipartFile
            , generateManifestFile, storeToCKAN)
        } else {
          null
        }

        val responseStatus = if(addDistributionResult != null) {
          addDistributionResult.errorCode;
        } else {
          null
        }


        val responseStatusText = if(addDistributionResult != null) {
          addDistributionResult.status
        } else {
          null
        }

        logger.debug(s"responseStatus = $responseStatus")
        logger.debug(s"responseStatusText = $responseStatusText")

        if (responseStatus == null || responseStatus < 200 || responseStatus >= 300) {
          val errorMessage = s"failed to add a distribution file: ${responseStatusText}"
          errorOccured = true;
          collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        }
      });
    }






    /*
    //STORING DISTRIBUTION FILE (IF SPECIFIED) ON GITHUB
    val addDistributionFileGitHubResponse:HttpResponse[JsonNode] = try {
      if(distribution != null) {
        distributionController.storeDatasetDistributionFileOnGitHub(distribution);
      } else {
        val statusMessage = "No distribution or distribution file has been provided"
        logger.info(statusMessage);
        null
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing dataset file on GitHub: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    val distributionAccessURL = if(addDistributionFileGitHubResponse == null) {
      null
    } else {
      this.githubClient.getAccessURL(addDistributionFileGitHubResponse)
    }
    val distributionDownloadURL = this.githubClient.getDownloadURL(distributionAccessURL);
    if(distributionDownloadURL != null) {
      distribution.sha = this.githubClient.getSHA(distributionAccessURL);
    }
    val addDatasetFileGitHubResponseStatus:Integer = if(addDistributionFileGitHubResponse == null) { null }
    else { addDistributionFileGitHubResponse.getStatus }

    val addDatasetFileGitHubResponseStatusText = if(addDistributionFileGitHubResponse == null) { null }
    else { addDistributionFileGitHubResponse.getStatusText }
    */

    //MANIFEST FILE
    val manifestFile:File = try {
      if (manifestFileRef != null) {//if the user provides a manifest file
        logger.info("Generating manifest file ... (manifestFileRef is not null)")
        val generatedFile = MappingPediaUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier)
        logger.info("Manifest file generated. (manifestFileRef is not null)")
        generatedFile
      } else { // if the user does not provide any manifest file
        if(generateManifestFile) {
          //MANIFEST FILE GENERATION
          val generatedFile = DatasetController.generateManifestFile(dataset);
          generatedFile
        } else {
          null
        }
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error generating manifest file: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }

    //STORING MANIFEST ON GITHUB
    val addManifestFileGitHubResponse:HttpResponse[JsonNode] = try {
      this.storeManifestFileOnGitHub(manifestFile, dataset);
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing manifest file on GitHub: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }

    //STORING MANIFEST ON VIRTUOSO
    val addManifestVirtuosoResponse:String = try {
      if(MappingPediaEngine.mappingpediaProperties.virtuosoEnabled) {
        if(manifestFile != null) {
          logger.info(s"STORING TRIPLES OF THE MANIFEST OF DATASET ${dataset.dctIdentifier} ON VIRTUOSO ...")
          MappingPediaEngine.virtuosoClient.storeFromFile(manifestFile)
          "OK"
        } else {
          "No manifest has been generated/provided";
        }
      } else {
        "Storing to Virtuoso is not enabled!";
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing manifest file on Virtuoso: " + e.getMessage
        logger.error(errorMessage);
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        e.getMessage
      }
    }






    /*
    //STORING DATASET & RESOURCE ON CKAN
    val (ckanAddPackageResponse:HttpResponse[JsonNode], ckanAddResourceResponse) = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable) {
        logger.info("storing dataset on CKAN ...")
        val addNewPackageResponse:HttpResponse[JsonNode] = ckanClient.addNewPackage(dataset);

        val (addResourceStatus, addResourceEntity) =
          if(distribution != null) {
            ckanClient.createResource(distribution);
          } else {
            (null, null)
          }

        if(addResourceStatus != null) {
          if (addResourceStatus.getStatusCode < 200 || addResourceStatus.getStatusCode >= 300) {
            val errorMessage = "failed to add the distribution file to CKAN storage. response status line from was: " + addResourceStatus
            throw new Exception(errorMessage);
          }
          logger.info("dataset stored on CKAN.")
        }

        (addNewPackageResponse, (addResourceStatus, addResourceEntity))
      } else {
        (null, null)
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing dataset file on CKAN: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    */

    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }



    dataset.manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse)
    dataset.manifestDownloadURL = this.githubClient.getDownloadURL(dataset.manifestAccessURL);
    logger.info(s"dataset.manifestAccessURL = ${dataset.manifestAccessURL}")
    logger.info(s"dataset.manifestDownloadURL = ${dataset.manifestDownloadURL}")

    val ckanAddPackageResponseStatusCode:Integer = {
      if(ckanAddPackageResponse == null) {
        null
      } else {
        ckanAddPackageResponse.getStatus
      }
    }
    /*    val ckanAddResourceResponseStatusCode:Integer = if(addDistributionResult == null) {
          null } else { addDistributionResult.ckanStoreResourceStatus }*/
    //distribution.ckanResourceId = if(addDistributionResult == null) { null } else {addDistributionResult.getDistribution_id }
    //val ckanResponseStatusText = ckanAddPackageResponseText + "," + ckanAddResourceResponseStatus;


    val addManifestFileGitHubResponseStatus:Integer = if(addManifestFileGitHubResponse == null) {
      null
    } else {
      addManifestFileGitHubResponse.getStatus
    }

    val addManifestFileGitHubResponseStatusText = if(addManifestFileGitHubResponse == null) {
      null
    } else {
      addManifestFileGitHubResponse.getStatusText
    }

    val addDatasetResult:AddDatasetResult = new AddDatasetResult(responseStatus, responseStatusText
      , dataset
      , addManifestFileGitHubResponseStatus, addManifestFileGitHubResponseStatusText
      //, distributionGithubStoreDistributionResponseStatus, distributionGithubStoreDistributionResponseStatusText
      , addManifestVirtuosoResponse
      , ckanAddPackageResponseStatusCode
      //, ckanAddResourceResponseStatusCode
    )

    try {
      val addDatasetResultAsJson = this.mapper.writeValueAsString(addDatasetResult);
      logger.info(s"addDatasetResultAsJson = ${addDatasetResultAsJson}\n\n");
    } catch {
      case e:Exception => {
        logger.error(s"addDatasetResult = ${addDatasetResult}")
      }
    }


    addDatasetResult

    /*
    val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
      , null, null, responseStatusText, responseStatus, ckanResponseStatusText)
    executionResult;
    */

  }
}

object DatasetController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  /*
  val ckanUtility = new CKANUtility(
    MappingPediaEngine.mappingpediaProperties.ckanURL, MappingPediaEngine.mappingpediaProperties.ckanKey)
  val githubClient = MappingPediaEngine.githubClient;
  */





  /*
    def storeManifestOnVirtuoso(manifestFile:File, message:String) = {
      if(manifestFile != null) {
        logger.info(s"storing manifest triples of the dataset ${dataset.dctIdentifier} on virtuoso ...")
        MappingPediaEngine.virtuosoClient.store(manifestFile)
        logger.info("manifest triples stored on virtuoso.")
        "OK";
      } else {
        "No manifest file specified/generated!";
      }
    }
  */

  def generateManifestFile(dataset: Dataset) = {
    logger.info(s"GENERATING MANIFEST FILE FOR DATASET ${dataset.dctIdentifier} ...")
    try {
      //val organization = dataset.dctPublisher;
      //val datasetDistribution = dataset.getDistribution();

      val templateFilesWithoutDistribution = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-dataset-template.ttl"
      );

      /*
      val templateFilesWithDistribution = if(datasetDistribution != null) {
        templateFilesWithoutDistribution :+ "templates/metadata-distributions-template.ttl"
      } else {
        templateFilesWithoutDistribution
      }
      */

      val datasetLanguage = if(dataset.dctLanguage != null) { dataset.dctLanguage } else { "" }


      val mapValuesWithoutDistribution:Map[String,String] = Map(
        "$publisherId" -> dataset.dctPublisher.dctIdentifier

        , "$datasetID" -> dataset.dctIdentifier
        , "$datasetTitle" -> dataset.dctTitle
        , "$datasetDescription" -> dataset.dctDescription
        , "$datasetKeywords" -> dataset.dcatKeyword
        , "$datasetLanguage" -> datasetLanguage
        , "$datasetIssued" -> dataset.dctIssued
        , "$datasetModified" -> dataset.dctModified
        , "$accessRight" -> dataset.dctAccessRight
        , "$provenance" -> dataset.dctProvenance

        , "$ckanPackageId" -> dataset.ckanPackageId
        , "$ckanPackageName" -> dataset.ckanPackageName
        , "$ckanSource" -> dataset.dctSource
        , "$ckanVersion" -> dataset.ckanVersion
        , "$ckanAuthor" -> dataset.getAuthor_name
        , "$ckanAuthorEmail" -> dataset.getAuthor_email
        , "$ckanMaintainer" -> dataset.getMaintainer_name
        , "$ckanMaintainerEmail" -> dataset.getMaintainer_email
        , "$ckanTemporal" -> dataset.ckanTemporal
        , "$ckanSpatial" -> dataset.ckanSpatial
        , "$ckanAccrualPeriodicity" -> dataset.ckanAccrualPeriodicity
        , "$ckanPackageLicense" -> dataset.ckanPackageLicense

        , "$provWasAttributedTo" -> dataset.provWasAttributedTo
        , "$provWasGeneratedBy" -> dataset.provWasGeneratedBy
        , "$provWasDerivedFrom" -> dataset.provWasDerivedFrom
        , "$provSpecializationOf" -> dataset.provSpecializationOf
        , "$provHadPrimarySource" -> dataset.provHadPrimarySource
        , "$provWasRevisionOf" -> dataset.provWasRevisionOf
        , "$provWasInfluencedBy" -> dataset.provWasInfluencedBy


      );

      /*
      val mapValuesWithDistribution:Map[String,String] = if(datasetDistribution != null) {
        var distributionAccessURL = datasetDistribution.dcatAccessURL
        if(distributionAccessURL != null && !distributionAccessURL.startsWith("<")) {
          distributionAccessURL = "<" + distributionAccessURL;
        }
        if(distributionAccessURL != null && !distributionAccessURL.endsWith(">")) {
          distributionAccessURL = distributionAccessURL + ">";
        }
        var distributionDownloadURL = datasetDistribution.dcatDownloadURL
        if(distributionDownloadURL != null && !distributionDownloadURL.startsWith("<")) {
          distributionDownloadURL = "<" + distributionDownloadURL;
        }
        if(distributionDownloadURL != null && !distributionDownloadURL.endsWith(">")) {
          distributionDownloadURL = distributionDownloadURL + ">";
        }
        mapValuesWithoutDistribution + (
          "$distributionID" -> datasetDistribution.dctIdentifier
          , "$distributionTitle" -> datasetDistribution.dctTitle
          , "$distributionDescription" -> datasetDistribution.dctDescription
          , "$distributionIssued" -> datasetDistribution.dctIssued
          , "$distributionModified" -> datasetDistribution.dctModified
          , "$distributionAccessURL" -> distributionAccessURL
          , "$distributionDownloadURL" -> distributionDownloadURL
          , "$distributionMediaType" -> datasetDistribution.dcatMediaType
          , "$sha" -> datasetDistribution.sha
        )
      } else {
        mapValuesWithoutDistribution
      }
      */

      /*
      val filename = if(datasetDistribution == null) {
        s"metadata-dataset-${dataset.dctIdentifier}.ttl"
      } else {
        s"metadata-distribution-${datasetDistribution.dctIdentifier}.ttl"
      };
      */
      val filename = s"metadata-dataset-${dataset.dctIdentifier}.ttl"

      val manifestFile = MappingPediaEngine.generateManifestFile(
        mapValuesWithoutDistribution, templateFilesWithoutDistribution, filename, dataset.dctIdentifier);
      logger.info("Manifest file generated.")
      manifestFile;
    } catch {
      case e:Exception => {
        e.printStackTrace()
        val errorMessage = "Error occured when generating manifest file: " + e.getMessage
        null;
      }
    }
  }





}

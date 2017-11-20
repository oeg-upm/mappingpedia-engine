package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.Date

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.sdf
import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController.logger
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController.logger
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddDatasetResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANClient, GitHubUtility, MappingPediaUtility}
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.util.EntityUtils
import org.json.JSONObject
import org.springframework.web.multipart.MultipartFile
import virtuoso.jena.driver.{VirtModel, VirtuosoQueryExecutionFactory}
import scala.collection.JavaConversions._

class DatasetController(val ckanClient:CKANClient, val githubClient:GitHubUtility)  {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val distributionController = new DistributionController(ckanClient, githubClient);



  def storeManifestFileOnGitHub(file:File, dataset:Dataset) = {
    val organization = dataset.dctPublisher;
    val datasetId = dataset.dctIdentifier;
    val manifestFileName = file.getName
    val organizationId = organization.dctIdentifier;

    logger.info(s"storing manifest file for the dataset ${datasetId} on github ...")
    val addNewManifestCommitMessage = s"Add manifest file for dataset: $datasetId"

    val githubResponse = githubClient.encodeAndPutFile(organization.dctIdentifier
      , datasetId, manifestFileName, addNewManifestCommitMessage, file)
    logger.info(s"Manifest file for dataset $datasetId stored on github ...")
    githubResponse
  }

  def addDataset(dataset:Dataset, manifestFileRef:MultipartFile, generateManifestFile:String
                ) : AddDatasetResult = {

    //val organization: Organization = dataset.dctPublisher;
    val distribution = dataset.getDistribution();
    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;

    //STORING DATASET AS PACKAGE ON CKAN
    val ckanAddPackageResponse:HttpResponse[JsonNode] = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable) {
        logger.info("storing dataset as a package on CKAN ...")
        ckanClient.addNewPackage(dataset);
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

    //CALLING ADD DISTRIBUTION IN DISTRIBUTIONCONTROLLER
    val addDistributionResult = if(distribution != null) {
      this.distributionController.addDistribution(
        distribution, manifestFileRef:MultipartFile, generateManifestFile:String)
    } else {
      null
    }
    val distributionGithubStoreDistributionResponseStatus = if(addDistributionResult != null) { addDistributionResult.githubStoreDistributionResponseStatus } else { null }
    val distributionGithubStoreDistributionResponseStatusText = if(addDistributionResult != null) { addDistributionResult.githubStoreDistributionResponseStatusText } else { null }




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
        if("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
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
          logger.info(s"storing manifest triples of the dataset ${dataset.dctIdentifier} on virtuoso ...")
          MappingPediaEngine.virtuosoClient.store(manifestFile)
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
    val ckanAddResourceResponseStatusCode:Integer = if(addDistributionResult == null) { null } else { addDistributionResult.ckanStoreResourceStatus }
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
      , distributionGithubStoreDistributionResponseStatus, distributionGithubStoreDistributionResponseStatusText
      , addManifestVirtuosoResponse
      , ckanAddPackageResponseStatusCode, ckanAddResourceResponseStatusCode
    )
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
    logger.info(s"Generating manifest file for dataset ${dataset.dctIdentifier} ...")
    try {
      val organization = dataset.dctPublisher;
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

      val mapValuesWithoutDistribution:Map[String,String] = Map(
        "$datasetID" -> dataset.dctIdentifier
        , "$datasetTitle" -> dataset.dctTitle
        , "$datasetDescription" -> dataset.dctDescription
        , "$datasetKeywords" -> dataset.dcatKeyword
        , "$publisherId" -> organization.dctIdentifier
        , "$datasetLanguage" -> dataset.dctLanguage
        , "$datasetIssued" -> dataset.dctIssued
        , "$datasetModified" -> dataset.dctModified
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


  def findDatasets(): ListResult = {
    logger.info("findDatasets")
    val queryTemplateFile = "templates/findAllDatasets.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    DatasetController.findDatasets(queryString);
  }

  def findDatasets(queryString: String): ListResult = {
    logger.info(s"queryString = $queryString");
    val m = VirtModel.openDatabaseModel(MappingPediaEngine.mappingpediaProperties.graphName, MappingPediaEngine.mappingpediaProperties.virtuosoJDBC
      , MappingPediaEngine.mappingpediaProperties.virtuosoUser, MappingPediaEngine.mappingpediaProperties.virtuosoPwd);



    val qexec = VirtuosoQueryExecutionFactory.create(queryString, m)
    var results: List[Dataset] = List.empty;
    try {
      val rs = qexec.execSelect
      while (rs.hasNext) {
        logger.info("Obtaining result from executing query=\n" + queryString)
        val qs = rs.nextSolution
        val datasetID = qs.get("datasetID").toString;
        val dataset = new Dataset(datasetID);
        val distributionID = qs.get("distributionID").toString;
        val distribution = new Distribution(dataset, distributionID);
        distribution.dcatAccessURL = MappingPediaUtility.getStringOrElse(qs, "distributionAccessURL", null)
        distribution.dcatDownloadURL = MappingPediaUtility.getStringOrElse(qs, "distributionDownloadURL", null)

        results = dataset :: results;
      }
    } finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }
}

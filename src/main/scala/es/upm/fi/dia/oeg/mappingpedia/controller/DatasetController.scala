package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.Date

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.sdf
import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController.logger
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.model.result.AddDatasetResult
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANClient, GitHubUtility, MappingPediaUtility}
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.util.EntityUtils
import org.json.JSONObject
import org.springframework.web.multipart.MultipartFile

class DatasetController(val ckanClient:CKANClient, val githubClient:GitHubUtility)  {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val distributionController = new DistributionController(ckanClient, githubClient);



  def storeManifestFileOnGitHub(file:File, dataset:Dataset) = {
    val organization = dataset.dctPublisher;

    logger.info("storing manifest file for a dataset on github ...")
    val manifestFileName = file.getName
    val datasetId = dataset.dctIdentifier;
    val organizationId = organization.dctIdentifier;
    val addNewManifestCommitMessage = s"Add manifest file for dataset: $datasetId"

    val githubResponse = githubClient.encodeAndPutFile(organizationId
      , datasetId, manifestFileName, addNewManifestCommitMessage, file)
    logger.info(s"Manifest file for dataset $datasetId stored on github ...")
    githubResponse
  }

  def addDataset(dataset:Dataset, manifestFileRef:MultipartFile, generateManifestFile:String
                ) : AddDatasetResult = {

    val organization: Organization = dataset.dctPublisher;
    val distribution = dataset.getDistribution();
    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;


    //STORING DISTRIBUTION FILE ON GITHUB
    val addDatasetFileGitHubResponse:HttpResponse[JsonNode] = try {
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
          DatasetController.storeManifestOnVirtuoso(manifestFile);
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

    val distributionAccessURL = if(addDatasetFileGitHubResponse == null) {
      null
    } else {
      addDatasetFileGitHubResponse.getBody.getObject.getJSONObject("content").getString("url")
    }
    val distributionDownloadURL = this.githubClient.getDownloadURL(distributionAccessURL);

    val manifestAccessURL = if(addManifestFileGitHubResponse == null) {
      null
    } else {
      addManifestFileGitHubResponse.getBody.getObject.getJSONObject("content").getString("url")
    }
    val manifestDownloadURL = this.githubClient.getDownloadURL(manifestAccessURL);

    val ckanAddPackageResponseStatusCode:Integer = {
      if(ckanAddPackageResponse == null) {
        null
      } else {
        ckanAddPackageResponse.getStatus
      }
    }
    val ckanAddResourceResponseStatusCode:Integer = {
      if(ckanAddResourceResponse == null) {
        null
      } else {
        ckanAddResourceResponse.getStatusLine.getStatusCode
      }
    }
    val ckanResourceId:String = {
      if(ckanAddResourceResponse == null) {
        null
      } else {
        val httpEntity  = ckanAddResourceResponse.getEntity
        val entity = EntityUtils.toString(httpEntity)
        val responseEntity = new JSONObject(entity);
        responseEntity.getJSONObject("result").getString("id");
      }
    }
    //val ckanResponseStatusText = ckanAddPackageResponseText + "," + ckanAddResourceResponseStatus;
    val addDatasetFileGitHubResponseStatus:Integer =
      if(addDatasetFileGitHubResponse == null) {
        null
      }  else {
        addDatasetFileGitHubResponse.getStatus
      }

    val addDatasetFileGitHubResponseStatusText =
      if(addDatasetFileGitHubResponse == null) {
        null
      }  else {
        addDatasetFileGitHubResponse.getStatusText
      }

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

    val addDatasetResult:AddDatasetResult = new AddDatasetResult(
      responseStatus, responseStatusText

      , manifestAccessURL, manifestDownloadURL
      , addManifestFileGitHubResponseStatus
      , addManifestFileGitHubResponseStatusText

      , distributionAccessURL, distributionDownloadURL
      , addDatasetFileGitHubResponseStatus
      , addDatasetFileGitHubResponseStatusText

      , addManifestVirtuosoResponse

      , ckanAddPackageResponseStatusCode
      , ckanAddResourceResponseStatusCode
      , ckanResourceId

      , dataset.dctIdentifier
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






  def storeManifestOnVirtuoso(manifestFile:File) = {
    if(manifestFile != null) {
      logger.info("storing the manifest triples of a dataset on virtuoso ...")
      logger.debug("manifestFile = " + manifestFile);
      MappingPediaEngine.virtuosoClient.store(manifestFile)
      logger.info("manifest triples stored on virtuoso.")
      "OK";
    } else {
      "No manifest file specified/generated!";
    }
  }

  def generateManifestFile(dataset: Dataset) = {
    logger.info("Generating dataset manifest file ...")
    try {
      val organization = dataset.dctPublisher;
      val datasetDistribution = dataset.getDistribution();

      val templateFilesWithoutDistribution = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-dataset-template.ttl"
      );
      val templateFilesWithDistribution = if(datasetDistribution != null) {
        templateFilesWithoutDistribution :+ "templates/metadata-distributions-template.ttl"
      } else {
        templateFilesWithoutDistribution
      }

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
        )
      } else {
        mapValuesWithoutDistribution
      }

      val filename = if(datasetDistribution == null) {
        "metadata-dataset.ttl"
      } else {
        s"metadata-distribution-${datasetDistribution.dctIdentifier}.ttl"
      };
      val manifestFile = MappingPediaEngine.generateManifestFile(
        mapValuesWithDistribution, templateFilesWithDistribution, filename, dataset.dctIdentifier);
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

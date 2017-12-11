package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddDatasetResult, AddDistributionResult}
import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution, Organization}
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANUtility, GitHubUtility, MappingPediaUtility}
import org.apache.http.util.EntityUtils
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

class DistributionController(val ckanClient:CKANUtility, val githubClient:GitHubUtility) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def storeManifestFileOnGitHub(file:File, distribution:Distribution) : HttpResponse[JsonNode] = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;


    logger.info(s"storing manifest file for distribution: ${distribution.dctIdentifier} - dataset: ${dataset.dctIdentifier} on github ...")
    val addNewManifestCommitMessage = s"Add manifest file for distribution: ${distribution.dctIdentifier} - dataset: ${dataset.dctIdentifier} "
    val manifestFileName = file.getName
    val datasetId = dataset.dctIdentifier;
    val organizationId = organization.dctIdentifier;

    val githubResponse = githubClient.encodeAndPutFile(organizationId
      , datasetId, manifestFileName, addNewManifestCommitMessage, file)
    logger.info("manifest file stored on github ...")
    githubResponse
  }

  def storeDatasetDistributionFileOnGitHub(distribution: Distribution) = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;

    val (filename:String, fileContent:String) =
      MappingPediaUtility.getFileNameAndContent(distribution.distributionFile, distribution.dcatDownloadURL, distribution.encoding);
    val base64EncodedContent = GitHubUtility.encodeToBase64(fileContent)


    logger.info("storing a distribution file on github ...")
    //val datasetFile = MappingPediaUtility.multipartFileToFile(distribution.ckanFileRef, dataset.dctIdentifier)
    val addNewDatasetCommitMessage = s"Add a new distribution file to dataset ${dataset.dctIdentifier}"
    val githubResponse = githubClient.putEncodedContent(organization.dctIdentifier
      , dataset.dctIdentifier, filename, addNewDatasetCommitMessage, base64EncodedContent)
    logger.info("New dataset file stored on github ...")

    if(githubResponse != null) {
      val responseStatus = githubResponse.getStatus;
      if (HttpURLConnection.HTTP_OK == responseStatus || HttpURLConnection.HTTP_CREATED == responseStatus) {
        logger.info("Dataset stored on GitHub")
        if(distribution.dcatAccessURL == null) {
          distribution.dcatAccessURL =
            githubResponse.getBody.getObject.getJSONObject("content").getString("url")
          logger.info(s"distribution.dcatAccessURL = ${distribution.dcatAccessURL}")
        }
        if(distribution.dcatDownloadURL == null) {
          distribution.dcatDownloadURL =
            githubResponse.getBody.getObject.getJSONObject("content").getString("download_url")
          logger.info(s"distribution.dcatDownloadURL = ${distribution.dcatDownloadURL}")
        }
      } else {
        val errorMessage = "Error when storing dataset on GitHub: " + responseStatus
        throw new Exception(errorMessage);
      }
    }

    githubResponse;
  }

  def addDistribution(distribution: Distribution, manifestFileRef:MultipartFile, generateManifestFile:String
                ) : AddDistributionResult = {

    //val dataset = distribution.dataset
    val organization: Organization = distribution.dataset.dctPublisher;
    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;


    //STORING DISTRIBUTION FILE ON GITHUB
    val addDistributionFileGitHubResponse:HttpResponse[JsonNode] = try {
      if(distribution != null) {
        this.storeDatasetDistributionFileOnGitHub(distribution);
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
    val addDistributionFileGitHubResponseStatus:Integer = if(addDistributionFileGitHubResponse == null) { null }
    else { addDistributionFileGitHubResponse.getStatus }

    if(addDistributionFileGitHubResponseStatus >= 200 && addDistributionFileGitHubResponseStatus < 300 && distributionDownloadURL != null) {
      distribution.sha = this.githubClient.getSHA(distributionAccessURL);
    }

    val addDatasetFileGitHubResponseStatusText = if(addDistributionFileGitHubResponse == null) { null }
    else { addDistributionFileGitHubResponse.getStatusText }




    /*
    //STORING DATASET & RESOURCE ON CKAN
    val ckanAddResourceResponse = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable) {

        logger.info("storing distribution on CKAN ...")
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

        (addResourceStatus, addResourceEntity)
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


    val ckanAddResourceResponseStatusCode:Integer = {
      if(ckanAddResourceResponse == null) {
        null
      } else {
        ckanAddResourceResponse.getStatusLine.getStatusCode
      }
    }
    distribution.ckanResourceId = {
      if(ckanAddResourceResponse == null) {
        null
      } else {
        val httpEntity  = ckanAddResourceResponse.getEntity
        val entity = EntityUtils.toString(httpEntity)
        val responseEntity = new JSONObject(entity);
        responseEntity.getJSONObject("result").getString("id");
      }
    }

    //MANIFEST FILE
    val manifestFile:File = try {
      if (manifestFileRef != null) {//if the user provides a manifest file
        logger.info("Generating manifest file ... (manifestFileRef is not null)")
        val generatedFile = MappingPediaUtility.multipartFileToFile(manifestFileRef, distribution.dataset.dctIdentifier)
        logger.info("Manifest file generated. (manifestFileRef is not null)")
        generatedFile
      } else { // if the user does not provide any manifest file
        if("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
          //MANIFEST FILE GENERATION
          val generatedFile = DistributionController.generateManifestFile(distribution);
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
      this.storeManifestFileOnGitHub(manifestFile, distribution);
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
    distribution.manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse)
    distribution.manifestDownloadURL = this.githubClient.getDownloadURL(distribution.manifestAccessURL);

    //STORING MANIFEST ON VIRTUOSO
    val addManifestVirtuosoResponse:String = try {
      if(MappingPediaEngine.mappingpediaProperties.virtuosoEnabled) {
        if(manifestFile != null) {
          logger.info(s"storing manifest triples of the distribution ${distribution.dctIdentifier} on virtuoso ...")
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

    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
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

    /*
    val addDatasetResult:AddDatasetResult = new AddDatasetResult(
      responseStatus, responseStatusText

      , manifestAccessURL, manifestDownloadURL
      , addManifestFileGitHubResponseStatus
      , addManifestFileGitHubResponseStatusText

      , distributionAccessURL, distributionDownloadURL, distribution.sha
      , addDatasetFileGitHubResponseStatus
      , addDatasetFileGitHubResponseStatusText

      , addManifestVirtuosoResponse

      , null
      , ckanAddResourceResponseStatusCode
      , ckanResourceId

      , distribution.dataset.dctIdentifier
    )
    addDatasetResult
    */

    val addDistributionResult:AddDistributionResult = new AddDistributionResult(responseStatus, responseStatusText
      , distribution

      //, manifestAccessURL, manifestDownloadURL
      , addManifestFileGitHubResponseStatus, addManifestFileGitHubResponseStatusText

      //, distributionAccessURL, distributionDownloadURL, distribution.sha
      , addDistributionFileGitHubResponseStatus, addDatasetFileGitHubResponseStatusText

      , addManifestVirtuosoResponse

      , ckanAddResourceResponseStatusCode
    )
    addDistributionResult


    /*
    val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
      , null, null, responseStatusText, responseStatus, ckanResponseStatusText)
    executionResult;
    */

  }
}

object DistributionController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def generateManifestFile(distribution: Distribution) = {
    logger.info("Generating distribution manifest file ...")
    val dataset = distribution.dataset;

    try {
      val organization = dataset.dctPublisher;

      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-distributions-template.ttl"
      );


      val mapValuesWithDistribution:Map[String,String] = {
        var distributionAccessURL = distribution.dcatAccessURL
        if(distributionAccessURL != null && !distributionAccessURL.startsWith("<")) {
          distributionAccessURL = "<" + distributionAccessURL;
        }
        if(distributionAccessURL != null && !distributionAccessURL.endsWith(">")) {
          distributionAccessURL = distributionAccessURL + ">";
        }
        var distributionDownloadURL = distribution.dcatDownloadURL
        if(distributionDownloadURL != null && !distributionDownloadURL.startsWith("<")) {
          distributionDownloadURL = "<" + distributionDownloadURL;
        }
        if(distributionDownloadURL != null && !distributionDownloadURL.endsWith(">")) {
          distributionDownloadURL = distributionDownloadURL + ">";
        }
        Map(
          "$datasetID" -> distribution.dataset.dctIdentifier
          , "$distributionTitle" -> distribution.dctTitle
          , "$distributionDescription" -> distribution.dctDescription
          , "$distributionAccessURL" -> distributionAccessURL
          , "$distributionDownloadURL" -> distributionDownloadURL
          , "$distributionMediaType" -> distribution.dcatMediaType
          , "$distributionID" -> distribution.dctIdentifier
          , "$distributionIssued" -> distribution.dctIssued
          , "$distributionModified" -> distribution.dctModified
          , "$ckanResourceID" -> distribution.ckanResourceId
          , "$sha" -> distribution.sha
        )
      }

      val filename = s"metadata-distribution-${distribution.dctIdentifier}.ttl"

      val manifestFile = MappingPediaEngine.generateManifestFile(
        mapValuesWithDistribution, templateFiles, filename, dataset.dctIdentifier);
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

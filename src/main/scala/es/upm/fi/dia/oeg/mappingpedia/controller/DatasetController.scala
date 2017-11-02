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
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility.logger
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANClient, GitHubUtility, MappingPediaUtility}
import org.springframework.web.multipart.MultipartFile

class DatasetController(val ckanClient:CKANClient, val githubClient:GitHubUtility)  {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def storeDatasetDistributionFileOnGitHub(distribution: Distribution) = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;

    val (filename:String, fileContent:String) =
      MappingPediaUtility.getFileNameAndContent(distribution.distributionFile, distribution.dcatDownloadURL, distribution.encoding);
    val base64EncodedContent = GitHubUtility.encodeToBase64(fileContent)


    logger.info("storing a new dataset and its distribution file on github ...")
    //val datasetFile = MappingPediaUtility.multipartFileToFile(distribution.ckanFileRef, dataset.dctIdentifier)
    val addNewDatasetCommitMessage = "Add a new dataset file by mappingpedia-engine"
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

  def storeManifestFileOnGitHub(manifestFile:File, dataset:Dataset) = {
    val organization = dataset.dctPublisher;

    logger.info("storing manifest file on github ...")
    val addNewManifestCommitMessage = "Add a new manifest file by mappingpedia-engine"
    val githubResponse = githubClient.encodeAndPutFile(organization.dctIdentifier
      , dataset.dctIdentifier, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
    logger.info("manifest file stored on github ...")
    githubResponse
  }

  def addDataset(dataset:Dataset, manifestFileRef:MultipartFile, generateManifestFile:String
                ) : AddDatasetResult = {

    val organization: Organization = dataset.dctPublisher;
    val distribution = dataset.getDistribution();
    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;


    //STORING DATASET FILE ON GITHUB
    val addDatasetFileGitHubResponse:HttpResponse[JsonNode] = try {
      this.storeDatasetDistributionFileOnGitHub(distribution);
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
          val generatedFile = DatasetController.generateManifestFile(distribution);
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


    //STORING MANIFEST ON VIRTUOSO
    val addManifestVirtuosoResponse:String = try {
      if(MappingPediaEngine.mappingpediaProperties.virtuosoEnabled) {
        DatasetController.storeManifestOnVirtuoso(manifestFile);
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


    //STORING DATASET & RESOURCE ON CKAN
    val (ckanAddPackageResponse:HttpResponse[JsonNode], ckanAddResourceResponse:Integer) = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable) {
        logger.info("storing dataset on CKAN ...")
        val addNewPackageResponse:HttpResponse[JsonNode] = ckanClient.addNewPackage(dataset);
        //val addNewResourceResponse = CKANUtility.addNewResource(distribution);
        val addNewResourceResponse:Integer = ckanClient.createResource(distribution);
        logger.info("dataset stored on CKAN.")
        (addNewPackageResponse, addNewResourceResponse)
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

    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }

    val datasetURL = if(addDatasetFileGitHubResponse == null) {
      null
    } else {
      addDatasetFileGitHubResponse.getBody.getObject.getJSONObject("content").getString("url")
    }

    val manifestURL = if(addManifestFileGitHubResponse == null) {
      null
    } else {
      addManifestFileGitHubResponse.getBody.getObject.getJSONObject("content").getString("url")
    }

    val ckanAddPackageResponseText = {
      if(ckanAddPackageResponse == null) {
        "null"
      } else {
        ckanAddPackageResponse.getStatusText
      }
    }
    val ckanResponseStatusText = ckanAddPackageResponseText + "," + ckanAddResourceResponse;

    val addDatasetResult:AddDatasetResult = new AddDatasetResult(
      responseStatus, responseStatusText

      , manifestURL
      , addManifestFileGitHubResponse.getStatus
      , addManifestFileGitHubResponse.getStatusText

      , datasetURL:String
      , addDatasetFileGitHubResponse.getStatus
      , addDatasetFileGitHubResponse.getStatusText

      , addManifestVirtuosoResponse

      , ckanAddPackageResponse.getStatus
      , ckanAddResourceResponse

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
      logger.info("storing the manifest triples on virtuoso ...")
      logger.debug("manifestFile = " + manifestFile);
      MappingPediaUtility.store(manifestFile, MappingPediaEngine.mappingpediaProperties.graphName)
      logger.info("manifest triples stored on virtuoso.")
      "OK";
    } else {
      "No manifest file specified/generated!";
    }
  }

  def generateManifestFile(distribution: Distribution) = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;

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

    logger.info("Generating manifest file ...")
    try {
      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-dataset-template.ttl"
        , "templates/metadata-distributions-template.ttl"
      );

      val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

      val mapValues:Map[String,String] = Map(
        "$datasetID" -> dataset.dctIdentifier
        , "$datasetTitle" -> dataset.dctTitle
        , "$datasetKeywords" -> dataset.dcatKeyword
        , "$publisherId" -> organization.dctIdentifier
        , "$datasetLanguage" -> dataset.dctLanguage
        , "$distributionID" -> dataset.dctIdentifier
        , "$distributionAccessURL" -> distributionAccessURL
        , "$distributionDownloadURL" -> distributionDownloadURL
        , "$distributionMediaType" -> distribution.dcatMediaType
      );

      val filename = "metadata-dataset.ttl";
      val manifestFile = MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, dataset.dctIdentifier);
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

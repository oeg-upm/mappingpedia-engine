package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.model.result.{AddDatasetResult, AddDistributionResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANUtility, GitHubUtility, MappingPediaUtility, VirtuosoClient}
import org.apache.http.util.EntityUtils
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

class DistributionController(val ckanClient:CKANUtility
                             , val githubClient:GitHubUtility
                             , val virtuosoClient: VirtuosoClient)
{
  val logger: Logger = LoggerFactory.getLogger(this.getClass);


  def findUnannotatedDistributions(queryString: String): ListResult = {
    logger.info(s"queryString = $queryString");

    val qexec = this.virtuosoClient.createQueryExecution(queryString);

    var results: List[Distribution] = List.empty;
    try {
      val rs = qexec.execSelect
      while (rs.hasNext) {
        val qs = rs.nextSolution

        val datasetId = qs.get("datasetId").toString;
        val dataset = new Dataset(datasetId);

        val distributionId = qs.get("distributionId").toString;
        val distribution = new UnannotatedDistribution(dataset, distributionId);

        distribution.dcatDownloadURL = MappingPediaUtility.getStringOrElse(qs, "distributionDownloadURL", null)

        results = distribution :: results;
      }
    }
    catch {
      case e:Exception => {
        e.printStackTrace()
        logger.error(s"Error execution query: ${e.getMessage}")
      }
    }
    finally qexec.close

    val listResult = new ListResult(results.length, results);
    listResult
  }

  def findDistributionByCKANResourceId(ckanResourceId:String): ListResult = {
    logger.info("findDistributionByCKANResourceId")
    val queryTemplateFile = "templates/findDistributionByCKANResourceId.rq";

    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$ckanResourceId" -> ckanResourceId
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(mapValues, queryTemplateFile)
    this.findUnannotatedDistributions(queryString);
  }

  def storeManifestFileOnGitHub(file:File, distribution:Distribution) : HttpResponse[JsonNode] = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;


    logger.info(s"STORING MANIFEST FILE FOR DISTRIBUTION: ${distribution.dctIdentifier} - DATASET: ${dataset.dctIdentifier} ON GITHUB ...")
    val addNewManifestCommitMessage = s"Add distribution manifest file: ${distribution.dctIdentifier}"
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

    val (filename:String, fileContent:String) = MappingPediaUtility.getFileNameAndContent(
      distribution.distributionFile, distribution.dcatDownloadURL, distribution.encoding);
    val base64EncodedContent = GitHubUtility.encodeToBase64(fileContent)


    logger.info("STORING DISTRIBUTION FILE ON GITHUB ...")
    //val datasetFile = MappingPediaUtility.multipartFileToFile(distribution.ckanFileRef, dataset.dctIdentifier)
    val addNewDatasetCommitMessage = s"Add distribution file to dataset: ${dataset.dctIdentifier}"
    val githubResponse = githubClient.putEncodedContent(organization.dctIdentifier
      , dataset.dctIdentifier, filename, addNewDatasetCommitMessage, base64EncodedContent)

    if(githubResponse != null) {
      val responseStatus = githubResponse.getStatus;
      if (HttpURLConnection.HTTP_OK == responseStatus || HttpURLConnection.HTTP_CREATED == responseStatus) {
        logger.info("Distribution file stored on GitHub")
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

  def addDistribution(distribution: Distribution, manifestFileRef:MultipartFile
                      , generateManifestFile:String, storeToCKAN:Boolean
                ) : AddDistributionResult = {

    //val dataset = distribution.dataset
    val organization: Agent = distribution.dataset.dctPublisher;
    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;


    //STORING DISTRIBUTION FILE ON GITHUB
    val addDistributionFileGitHubResponse:HttpResponse[JsonNode] = try {
      if(distribution != null &&
        (distribution.distributionFile != null || distribution.dcatDownloadURL != null)) {
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
    /*
    if(addDistributionFileGitHubResponseStatus!= null && addDistributionFileGitHubResponseStatus >= 200
      && addDistributionFileGitHubResponseStatus < 300 && distributionDownloadURL != null) {
      distribution.hash = this.githubClient.getSHA(distributionAccessURL);
    }
    */
    distribution.hash = MappingPediaUtility.calculateHash(distributionDownloadURL, distribution.encoding);


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
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable && storeToCKAN) {
        logger.info("STORING DISTRIBUTION FILE AS A RESOURCE ON CKAN...")

        if(distribution != null
          && (distribution.distributionFile != null || distribution.dcatDownloadURL != null)) {
          ckanClient.createResource(distribution, None);
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
    logger.info(s"ckanAddResourceResponse= ${ckanAddResourceResponse}");

    val ckanAddResourceResponseStatusCode:Integer = {
      if(ckanAddResourceResponse == null) {
        null
      } else {
        ckanAddResourceResponse.getStatusLine.getStatusCode
      }
    }

/*    distribution.ckanResourceId = {
      if(ckanAddResourceResponse == null) {
        null
      } else {
        val httpEntity  = ckanAddResourceResponse.getEntity
        val entity = EntityUtils.toString(httpEntity)
        val responseEntity = new JSONObject(entity);
        responseEntity.getJSONObject("result").getString("id");
      }
    }*/
    distribution.ckanResourceId = CKANUtility.getResultId(ckanAddResourceResponse);
    logger.info(s"distribution.ckanResourceId = ${distribution.ckanResourceId}");

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
          logger.info(s"STORING TRIPLES OF THE MANIFEST FILE FOR DISTRIBUTION ${distribution.dctIdentifier} ON VIRTUOSO...")
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

    logger.debug(s"errorOccured = $errorOccured")
    logger.debug(s"collectiveErrorMessage = $collectiveErrorMessage")

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
    logger.info("GENERATING MANIFEST FOR DISTRIBUTION ...")
    val dataset = distribution.dataset;

    try {
      val organization = dataset.dctPublisher;

      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-distributions-template.ttl"
      );


      val mapValuesWithDistribution:Map[String,String] = {
        val distributionAccessURL = if(distribution.dcatAccessURL == null) { ""; }
        else { distribution.dcatAccessURL }

        logger.info(s"distributionAccessURL = ${distributionAccessURL}")

        val distributionDownloadURL = if(distribution.dcatDownloadURL == null) { ""; }
        else { distribution.dcatDownloadURL }

        logger.info(s"distributionDownloadURL = ${distributionDownloadURL}")

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
          , "$hash" -> distribution.hash
          , "$distributionLanguage" -> distribution.dctLanguage
          , "$distributionLicense" -> distribution.dctLicense
          , "$distributionRights" -> distribution.dctRights
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

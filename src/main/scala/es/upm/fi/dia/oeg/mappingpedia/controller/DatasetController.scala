package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.Date

import com.mashape.unirest.http.{HttpResponse, JsonNode}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.{logger, sdf}
import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution, MappingPediaExecutionResult, Organization}
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANUtility, GitHubUtility, MappingPediaUtility}
import org.springframework.web.multipart.MultipartFile


object DatasetController {
  def generateManifestFile(distribution: Distribution) = {
    val dataset = distribution.dataset;
    val organization = dataset.dctPublisher;

    //GENERATE MANIFEST FILE IF NOT PROVIDED
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

    logger.info("generating manifest file ...")
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
      MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, dataset.dctIdentifier);
    } catch {
      case e:Exception => {
        e.printStackTrace()
        val errorMessage = "Error occured when generating manifest file: " + e.getMessage
        null;
      }
    }

  }

  def addDataset(dataset:Dataset, manifestFileRef:MultipartFile, generateManifestFile:String
                ) : MappingPediaExecutionResult = {

    val organization: Organization = dataset.dctPublisher;
    val distribution = dataset.getDistribution();
    var errorOccured = false;
    var collectiveErrorMessage:List[String] = Nil;


    //MANIFEST FILE GENERATION
    val manifestFile:File = try {
      if (manifestFileRef != null) {//if the user provides a manifest file
        logger.info("Generating manifest file ... (manifestFileRef is not null)")
        val generatedFile = MappingPediaUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier)
        logger.info("Manifest file generated. (manifestFileRef is not null)")
        generatedFile
      } else { // if the user does not provide any manifest file
        if("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
          logger.info("Generating manifest file ...")
          val generatedFile = this.generateManifestFile(distribution);
          logger.info("Manifest file generated.")
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
    //val manifest_in_str = scala.io.Source.fromFile(manifestFile).getLines.reduceLeft(_+_)

    //STORING MANIFEST ON VIRTUOSO
    try {
      if(manifestFile != null) {
        logger.info("storing the manifest triples on virtuoso ...")
        logger.debug("manifestFile = " + manifestFile);
        MappingPediaUtility.store(manifestFile, MappingPediaEngine.mappingpediaProperties.graphName)
        logger.info("manifest triples stored on virtuoso.")
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing manifest file on Virtuoso: " + e.getMessage
        logger.error(errorMessage);
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
      }
    }


    //STORING DATASET FILE ON GITHUB
    val addNewDatasetResponse:HttpResponse[JsonNode] = try {
      if(distribution.ckanFileRef != null) {
        logger.info("storing a new dataset file on github ...")
        val datasetFile = MappingPediaUtility.multipartFileToFile(distribution.ckanFileRef, dataset.dctIdentifier)
        val addNewDatasetCommitMessage = "Add a new dataset file by mappingpedia-engine"
        val githubResponse = GitHubUtility.putEncodedFile(MappingPediaEngine.mappingpediaProperties.githubUser
          , MappingPediaEngine.mappingpediaProperties.githubAccessToken, organization.dctIdentifier
          , dataset.dctIdentifier, datasetFile.getName, addNewDatasetCommitMessage, datasetFile)
        logger.info("New dataset file stored on github ...")
        githubResponse;
      } else {
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
    val datasetURL = if(addNewDatasetResponse == null) {
      null
    } else {
      addNewDatasetResponse.getBody.getObject.getJSONObject("content").getString("url")
    }


    //STORING MANIFEST ON GITHUB
    val addManifestFileGitHubResponse:HttpResponse[JsonNode] = try {
      logger.info("storing manifest file on github ...")
      val addNewManifestCommitMessage = "Add a new manifest file by mappingpedia-engine"
      val githubResponse = GitHubUtility.putEncodedFile(MappingPediaEngine.mappingpediaProperties.githubUser
        , MappingPediaEngine.mappingpediaProperties.githubAccessToken, organization.dctIdentifier
        , dataset.dctIdentifier, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
      logger.info("manifest file stored on github ...")
      githubResponse
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
    val manifestURL = if(addManifestFileGitHubResponse == null) {
      null
    } else {
      addManifestFileGitHubResponse.getBody.getObject.getJSONObject("content").getString("url")
    }


    //STORING DATASET & RESOURCE ON CKAN
    val ckanResponse = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable) {
        logger.info("storing dataset on CKAN ...")
        val addNewPackageResponse = CKANUtility.addNewPackage(organization, dataset);
        val addNewResourceResponse = CKANUtility.addNewResource(dataset, distribution);
        logger.info("dataset stored on CKAN.")
        (addNewPackageResponse, addNewResourceResponse)
      } else {
        null
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
    val ckanResponseStatusText = if(ckanResponse == null) {
      null
    }  else {
      ckanResponse._1.getStatusText + "," + ckanResponse._2.getStatusText;
    }


    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }

    val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
      , null, null, responseStatusText, responseStatus, ckanResponseStatusText)
    executionResult;

  }

}

package es.upm.fi.dia.oeg.mappingpedia

import java.io.File
import java.net.HttpURLConnection
import java.util.{Date, UUID}

import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.{logger, sdf}
import es.upm.fi.dia.oeg.mappingpedia.model.MappingPediaExecutionResult
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANUtility, GitHubUtility, MappingPediaUtility}
import org.springframework.web.multipart.MultipartFile

object Dataset {
  def addDatasetFile(datasetFileRef: MultipartFile, manifestFileRef:MultipartFile, generateManifestFile:String
                     , mappingpediaUsername:String, datasetTitle:String, datasetKeywords:String, publisherId:String, datasetLanguage:String
                     , distributionAccessURL:String, distributionDownloadURL:String, distributionMediaType:String
                    ) : MappingPediaExecutionResult = {

    val datasetID = UUID.randomUUID.toString;
    this.addDatasetFileWithID(datasetFileRef, manifestFileRef, generateManifestFile
      , datasetID, datasetTitle, datasetKeywords, publisherId, datasetLanguage
      , distributionAccessURL, distributionDownloadURL, distributionMediaType
    );
  }

  def addDatasetFileWithID(datasetFileRef: MultipartFile, manifestFileRef:MultipartFile, generateManifestFile:String
                           , datasetID:String, datasetTitle:String, datasetKeywords:String, publisherId:String, datasetLanguage:String
                           , pDistributionAccessURL:String, pDistributionDownloadURL:String, distributionMediaType:String
                          ) : MappingPediaExecutionResult = {
    logger.debug("datasetID = " + datasetID)

    var distributionAccessURL = pDistributionAccessURL;
    if(distributionAccessURL != null && !distributionAccessURL.startsWith("<")) {
      distributionAccessURL = "<" + distributionAccessURL;
    }
    if(distributionAccessURL != null && !distributionAccessURL.endsWith(">")) {
      distributionAccessURL = distributionAccessURL + ">";
    }
    var distributionDownloadURL = pDistributionDownloadURL;
    if(distributionDownloadURL != null && !distributionDownloadURL.startsWith("<")) {
      distributionDownloadURL = "<" + distributionDownloadURL;
    }
    if(distributionDownloadURL != null && !distributionDownloadURL.endsWith(">")) {
      distributionDownloadURL = distributionDownloadURL + ">";
    }

    try {
      val manifestFile:File = if (manifestFileRef != null) {
        MappingPediaUtility.multipartFileToFile(manifestFileRef, datasetID)
      } else {
        //GENERATE MANIFEST FILE IF NOT PROVIDED
        if("true".equalsIgnoreCase(generateManifestFile) || "yes".equalsIgnoreCase(generateManifestFile)) {
          logger.info("generating manifest file ...")
          try {
            val templateFiles = List(
              "templates/metadata-namespaces-template.ttl"
              , "templates/metadata-dataset-template.ttl"
              , "templates/metadata-distributions-template.ttl"
            );

            val mappingDocumentDateTimeSubmitted = sdf.format(new Date())

            val mapValues:Map[String,String] = Map(
              "$datasetID" -> datasetID
              , "$datasetTitle" -> datasetTitle
              , "$datasetKeywords" -> datasetKeywords
              , "$publisherId" -> publisherId
              , "$datasetLanguage" -> datasetLanguage
              , "$distributionID" -> datasetID
              , "$distributionAccessURL" -> distributionAccessURL
              , "$distributionDownloadURL" -> distributionDownloadURL
              , "$distributionMediaType" -> distributionMediaType
            );

            val filename = "metadata-dataset.ttl";
            MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, datasetID);
          } catch {
            case e:Exception => {
              e.printStackTrace()
              val errorMessage = "Error occured when generating manifest file: " + e.getMessage
              null;
            }
          }
        } else {
          null
        }
      }


      //STORING MANIFEST ON VIRTUOSO
      if(manifestFile != null) {
        logger.info("storing the manifest-dataset triples on virtuoso ...")
        logger.debug("manifestFile = " + manifestFile);
        MappingPediaUtility.store(manifestFile, MappingPediaEngine.mappingpediaProperties.graphName)
      }


      val optionDatasetFile:Option[File] = if(datasetFileRef == null) {
        None
      }  else {
        Some(MappingPediaUtility.multipartFileToFile(datasetFileRef, datasetID))
      }


      //STORING DATASET ON GITHUB
      var datasetURL:String = null;
      val addNewDatasetResponseStatus = if(optionDatasetFile.isDefined) {
        logger.info("storing a new dataset file on github ...")
        val datasetFile = optionDatasetFile.get;
        val addNewDatasetCommitMessage = "Add a new dataset file by mappingpedia-engine"
        val addNewDatasetResponse = GitHubUtility.putEncodedFile(MappingPediaEngine.mappingpediaProperties.githubUser
          , MappingPediaEngine.mappingpediaProperties.githubAccessToken, publisherId
          , datasetID, datasetFile.getName, addNewDatasetCommitMessage, datasetFile)
        val addNewDatasetResponseStatus = addNewDatasetResponse.getStatus

        if (HttpURLConnection.HTTP_CREATED == addNewDatasetResponseStatus) {
          datasetURL = addNewDatasetResponse.getBody.getObject.getJSONObject("content").getString("url")
        }
        addNewDatasetResponseStatus;
      } else {
        HttpURLConnection.HTTP_OK;
      }

      //STORING MANIFEST ON GITHUB
      val manifestURL:String = if(manifestFile == null) {
        null
      } else {
        logger.info("storing manifest-dataset file on github ...")
        val addNewManifestCommitMessage = "Add a new manifest file by mappingpedia-engine"
        val addNewManifestResponse = GitHubUtility.putEncodedFile(MappingPediaEngine.mappingpediaProperties.githubUser
          , MappingPediaEngine.mappingpediaProperties.githubAccessToken, publisherId
          , datasetID, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
        val addNewManifestResponseStatus = addNewManifestResponse.getStatus
        logger.info("addNewManifestResponseStatus = " + addNewManifestResponseStatus)

        if (HttpURLConnection.HTTP_CREATED == addNewManifestResponseStatus) {
          addNewManifestResponse.getBody.getObject.getJSONObject("content").getString("url")
        } else {
          null
        }
      }

      //STORING DATASET ON CKAN
      val ckanResponse = if(MappingPediaEngine.mappingpediaProperties.ckanEnable) {
        logger.info("storing dataset on CKAN ...")
        CKANUtility.addNewDataset(datasetID, publisherId, datasetTitle)
      } else {
        null
      }


      if(HttpURLConnection.HTTP_CREATED == addNewDatasetResponseStatus || HttpURLConnection.HTTP_OK == addNewDatasetResponseStatus) {
        val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
          , null, null, "OK", HttpURLConnection.HTTP_OK, ckanResponse.getStatusText)
        return executionResult;
      } else {
        val executionResult = new MappingPediaExecutionResult(manifestURL, datasetURL, null
          , null, null, "Internal Error", HttpURLConnection.HTTP_INTERNAL_ERROR, ckanResponse.getStatusText)
        return executionResult;
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        val errorMessage = e.getMessage
        logger.error("error uploading a new file: " + errorMessage)
        val errorCode = HttpURLConnection.HTTP_INTERNAL_ERROR
        val executionResult = new MappingPediaExecutionResult(null, null, null
          , null, null, errorMessage, errorCode, null)
        return executionResult
    }
  }

}

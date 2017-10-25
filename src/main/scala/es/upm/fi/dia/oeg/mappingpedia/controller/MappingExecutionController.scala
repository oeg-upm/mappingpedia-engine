package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.UUID

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController.logger
import es.upm.fi.dia.oeg.mappingpedia.model.result.{ExecuteMappingResult, GeneralResult}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
//import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.logger
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANUtility, GitHubUtility}
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory}
import org.apache.commons.lang.text.StrSubstitutor

object MappingExecutionController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  @throws(classOf[Exception])
  def executeMapping(
                       md:MappingDocument
                       , dataset:Dataset
                       , queryFileName:String
                       , pOutputFilename: String
                       , pStoreToCKAN:Boolean
                     ) : ExecuteMappingResult = {
    var errorOccured = false;
    var collectiveErrorMessage: List[String] = Nil;

    val organization = dataset.dctPublisher;
    val distribution = dataset.getDistribution();
    val distributionDownloadURL = distribution.dcatDownloadURL;

    val mdDownloadURL = md.getDownloadURL();
    val mappingLanguage = if (md.mappingLanguage == null) {
      MappingPediaConstant.MAPPING_LANGUAGE_R2RML
    } else {
      md.mappingLanguage
    }

    val mappingExecutionDirectory = if(organization != null && dataset != null) {
      organization.dctIdentifier + File.separator + dataset.dctIdentifier
    } else { UUID.randomUUID.toString }

    val outputFileName = if (pOutputFilename == null) {
      UUID.randomUUID.toString + ".txt"
    } else {
      pOutputFilename;
    }
    val outputFilepath = "executions/" + mappingExecutionDirectory + "/" + outputFileName

    //EXECUTING MAPPING
    try {
      if (MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {
        logger.info("Executing R2RML mapping ...")
        this.executeR2RMLMapping(md, dataset, outputFilepath, queryFileName);
      } else if (MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
        logger.info("Executing RML mapping ...")
        val rmlConnector = new RMLMapperConnector();
        rmlConnector.executeWithMain(distributionDownloadURL, mdDownloadURL, outputFilepath);
      } else if (MappingPediaConstant.MAPPING_LANGUAGE_xR2RML.equalsIgnoreCase(mappingLanguage)) {
        throw new Exception(mappingLanguage + " Language is not supported yet");
      } else {
        throw new Exception(mappingLanguage + " Language is not supported yet");
      }
      logger.info("mapping execution success!")
    }
    catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "Error executing mapping: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
      }
    }

    //STORING MAPPING EXECUTION RESULT ON GITHUB
    val githubResponse = try {
      val outputFile: File = new File(outputFilepath)
      val response = GitHubUtility.encodeAndPutFile(MappingPediaEngine.mappingpediaProperties.githubUser
        , MappingPediaEngine.mappingpediaProperties.githubAccessToken
        , "executions", mappingExecutionDirectory, outputFileName
        , "add mapping execution result by mappingpedia engine", outputFile);
      response
    } catch {
      case e: Exception => {
        e.printStackTrace()
        errorOccured = true;
        val errorMessage = "Error storing mapping execution result on GitHub: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    val (mappingExecutionResultURL, mappingExecutionResultDownloadURL) = if(githubResponse != null) {
      val responseStatus: Int = githubResponse.getStatus
      logger.info("responseStatus = " + responseStatus)
      val responseStatusText: String = githubResponse.getStatusText
      logger.info("responseStatusText = " + responseStatusText)

      if (HttpURLConnection.HTTP_CREATED == responseStatus || HttpURLConnection.HTTP_OK == responseStatus) {
        val url: String = githubResponse.getBody.getObject.getJSONObject("content").getString("url");
        val downloadURL = try {
          val response = Unirest.get(url).asJson();
          response.getBody.getObject.getString("download_url");
        } catch {
          case e:Exception => url
        }
        (url, downloadURL)
      } else {
        errorOccured = true;
        val errorMessage = "Error storing mapping execution result on GitHub: " + responseStatus
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        (null, null)
      }
    } else {
      (null, null)
    }

    //STORING DATASET & RESOURCE ON CKAN
    val ckanResponse = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable && pStoreToCKAN) {
        logger.info("storing dataset on CKAN ...")

        val distribution = new Distribution(dataset)
        distribution.dcatAccessURL = mappingExecutionResultURL;
        distribution.dcatDownloadURL = mappingExecutionResultDownloadURL;
        distribution.dcatMediaType = null //TODO FIXME
        distribution.ckanFileRef = null;
        distribution.ckanDescription = "Mapping Execution Result";


        //val addNewResourceResponse = CKANUtility.addNewResource(resourceIdentifier, resourceTitle
        //            , resourceMediaType, resourceFileRef, resourceDownloadURL)
        val addNewResourceResponse = CKANUtility.addNewResource(dataset, distribution);

        logger.info("dataset stored on CKAN.")
        addNewResourceResponse
      } else {
        null
      }
    }
    catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "Error storing mapping execution result on CKAN: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    }
    val ckanResponseText = if(ckanResponse == null) { null}
    else { ckanResponse.getStatusText}

    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }

    new ExecuteMappingResult(
      responseStatus, responseStatusText
      , distributionDownloadURL, mdDownloadURL
      , queryFileName
      , mappingExecutionResultURL, mappingExecutionResultDownloadURL
      , ckanResponseText:String
    )

    /*
    new GeneralResult(null
      , distributionDownloadURL, mdDownloadURL, queryFileName
      , mappingExecutionResultURL, responseStatusText, responseStatus, ckanResponseText)
      */

  }

  def executeR2RMLMapping(md:MappingDocument, dataset: Dataset, outputFilepath:String, queryFileName:String) = {
    val distribution = dataset.getDistribution();
    val randomUUID = UUID.randomUUID.toString
    val databaseName =  s"executions/${md.dctIdentifier}/${randomUUID}"
    logger.info(s"databaseName = $databaseName")

    val properties: MorphCSVProperties = new MorphCSVProperties
    properties.setDatabaseName(databaseName)
    properties.setMappingDocumentFilePath(md.getDownloadURL())
    properties.setOutputFilePath(outputFilepath);
    properties.setCSVFile(distribution.dcatDownloadURL);
    properties.setQueryFilePath(queryFileName);
    if (distribution.cvsFieldSeparator != null) {
      properties.fieldSeparator = Some(distribution.cvsFieldSeparator);
    }

    val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
    val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
    runner.run
  }

}

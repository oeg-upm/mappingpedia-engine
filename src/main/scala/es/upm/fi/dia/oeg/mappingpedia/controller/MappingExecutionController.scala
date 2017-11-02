package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.UUID

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.logger
import es.upm.fi.dia.oeg.mappingpedia.model.result.{ExecuteMappingResult, GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController.logger
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANClient, GitHubUtility, MappingPediaUtility}
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory}
import scala.collection.JavaConversions._

class MappingExecutionController(val ckanClient:CKANClient, val githubClient:GitHubUtility) {
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
    val datasetDistribution = dataset.getDistribution();
    val datasetDistributionDownloadURL = datasetDistribution.dcatDownloadURL;

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
    val outputFile: File = new File(outputFilepath)

    //EXECUTING MAPPING
    try {
      if (MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {
        logger.info("Executing R2RML mapping ...")
        MappingExecutionController.executeR2RMLMapping(md, dataset, outputFilepath, queryFileName);
      } else if (MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
        logger.info("Executing RML mapping ...")
        val rmlConnector = new RMLMapperConnector();
        rmlConnector.executeWithMain(datasetDistributionDownloadURL, mdDownloadURL, outputFilepath);
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
      val response = githubClient.encodeAndPutFile("executions", mappingExecutionDirectory, outputFileName
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

    //STORING MAPPING EXECUTION RESULT ON CKAN
    val ckanResponseStatusCode = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable && pStoreToCKAN) {
        logger.info("storing mapping execution result on CKAN ...")

        val mappingExecutionResultDistribution = new Distribution(dataset)
        mappingExecutionResultDistribution.dcatAccessURL = mappingExecutionResultURL;
        mappingExecutionResultDistribution.dcatDownloadURL = mappingExecutionResultDownloadURL;
        mappingExecutionResultDistribution.dcatMediaType = null //TODO FIXME
        mappingExecutionResultDistribution.dctDescription = "Annotated Dataset using the annotation:" + mdDownloadURL;
        mappingExecutionResultDistribution.distributionFile = outputFile;


        //val addNewResourceResponse = CKANUtility.addNewResource(resourceIdentifier, resourceTitle
        //            , resourceMediaType, resourceFileRef, resourceDownloadURL)
        //val addNewResourceResponse = CKANUtility.addNewResource(distribution);
        val (addNewResourceStatus, addNewResourceEntity) = ckanClient.createResource(mappingExecutionResultDistribution);

        logger.info("mapping execution result stored on CKAN.")
        addNewResourceStatus.getStatusCode
      } else {
        HttpURLConnection.HTTP_OK;
      }
    }
    catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "Error storing mapping execution result on CKAN: " + e.getMessage
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        HttpURLConnection.HTTP_INTERNAL_ERROR
      }
    }

    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }

    new ExecuteMappingResult(
      responseStatus, responseStatusText
      , datasetDistributionDownloadURL, mdDownloadURL
      , queryFileName
      , mappingExecutionResultURL, mappingExecutionResultDownloadURL
      , ckanResponseStatusCode
    )

    /*
    new GeneralResult(null
      , distributionDownloadURL, mdDownloadURL, queryFileName
      , mappingExecutionResultURL, responseStatusText, responseStatus, ckanResponseText)
      */

  }

  def getInstances(aClass:String, outputType:String, inputType:String) : ListResult = {
    val subclassesListResult = MappingPediaUtility.getSubclassesDetail(
      aClass, MappingPediaEngine.schemaOrgModel, outputType, inputType);
    logger.info(s"subclassesListResult = subclassesListResult")

    val subclassesURIs:Iterable[String] = subclassesListResult.results.map(
      result => result.asInstanceOf[OntologyClass].getURI).toList.distinct
    //		val subclassesInList:Iterable[String] = subclassesListResult.results.values.map(
    //      result => result.asInstanceOf[OntologyClass].aClass).toList.distinct

    logger.debug("subclassesInList" + subclassesURIs)
    //new ListResult(subclassesInList.size, subclassesInList);
    val queryFile:String = null;

    val mappingDocuments:Iterable[MappingDocument] = subclassesURIs.flatMap(subclassURI => {
      val mappingDocumentsByClassURI =
        MappingDocumentController.findMappingDocumentsByMappedClass(subclassURI).getResults();
      mappingDocumentsByClassURI
    }).asInstanceOf[Iterable[MappingDocument]];

    var executedMappings:List[(String, String)]= Nil;

    val executionResults:Iterable[Execution] = mappingDocuments.flatMap(mappingDocument => {
      val md = mappingDocument.asInstanceOf[MappingDocument];
      val mappingLanguage = md.mappingLanguage;
      val distributionFieldSeparator = if(md.distributionFieldSeparator != null && md.distributionFieldSeparator.isDefined) {
        md.distributionFieldSeparator.get
      } else {
        null
      }
      val outputFilename = UUID.randomUUID.toString + ".nt"
      val mappingDocumentDownloadURL = md.getDownloadURL();
      logger.info("mappingDocumentDownloadURL = " + mappingDocumentDownloadURL);
      val mdDistributionAccessURL = md.distributionAccessURL;
      logger.info("mdDistributionAccessURL = " + mdDistributionAccessURL);

      if(mappingDocumentDownloadURL != null && mdDistributionAccessURL != null) {
        if(executedMappings.contains((mappingDocumentDownloadURL,mdDistributionAccessURL))) {
          None
        } else {
          val dataset = new Dataset(new Organization());
          val distribution = new Distribution(dataset);
          dataset.addDistribution(distribution);
          distribution.dcatDownloadURL = mdDistributionAccessURL;

          val mappingExecution = new MappingExecution(md, dataset);
          mappingExecution.setStoreToCKAN("false")
          mappingExecution.queryFilePath = queryFile;
          mappingExecution.outputFileName = outputFilename;

          //THERE IS NO NEED TO STORE THE EXECUTION RESULT IN THIS PARTICULAR CASE
          val executionResult = this.executeMapping(md, dataset, queryFile, outputFilename, false);
          //val executionResult = MappingExecutionController.executeMapping2(mappingExecution);

          executedMappings = (mappingDocumentDownloadURL,mdDistributionAccessURL) :: executedMappings;

          val executionResultURL = executionResult.mappingExecutionResultDownloadURL;
          //executionResultURL;

          Some(new Execution(mappingDocumentDownloadURL, mdDistributionAccessURL, executionResultURL, executionResultURL))
          //mappingDocumentURL + " -- " + datasetDistributionURL
        }
      } else {
        None
      }


    })
    new ListResult(executionResults.size, executionResults);

  }

  def getInstances(aClass:String) : ListResult = {
    this.getInstances(aClass, "0", "0")
  }
}

object MappingExecutionController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  /*
  val ckanUtility = new CKANUtility(
    MappingPediaEngine.mappingpediaProperties.ckanURL, MappingPediaEngine.mappingpediaProperties.ckanKey)
  val githubClient = MappingPediaEngine.githubClient;
  */



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

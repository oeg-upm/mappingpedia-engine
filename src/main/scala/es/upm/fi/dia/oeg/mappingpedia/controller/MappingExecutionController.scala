package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.{Date, UUID}

import com.mashape.unirest.http.{HttpResponse, JsonNode, Unirest}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.{logger, sdf}
import es.upm.fi.dia.oeg.mappingpedia.model.result.{ExecuteMappingResult, ExecutionMappingResultSummary, GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController.logger
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController.logger
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility.{CKANUtility, GitHubUtility, JenaClient, MappingPediaUtility}
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory, MorphRDBProperties, MorphRDBRunnerFactory}

import scala.collection.JavaConversions._

class MappingExecutionController(val ckanClient:CKANUtility, val githubClient:GitHubUtility) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  @throws(classOf[Exception])
  def executeMapping(
                      md:MappingDocument
                      , dataset:Dataset
                      , queryFileName:String
                      , pOutputFilename: String

                      , pStoreToCKAN:Boolean
                      , pStoreToGithub:Boolean
                      , pStoreToVirtuoso:Boolean

                    /*
                      , dbUserName:String, dbPassword:String
                      , dbName:String, jdbc_url:String
                      , databaseDriver:String, databaseType:String
                      */
                    , jdbcConnection: JDBCConnection
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

    val organizationId = if(organization != null) {
      organization.dctIdentifier
    } else {
      UUID.randomUUID.toString
    }

    val datasetId = if(dataset != null) {
      dataset.dctIdentifier
    } else {
      UUID.randomUUID.toString
    }

    val mappingExecutionId = UUID.randomUUID.toString;

    /*
    val mappingExecutionDirectory = if(organization != null && dataset != null) {
      organization.dctIdentifier + File.separator + dataset.dctIdentifier
    } else { UUID.randomUUID.toString }
    */

    val mappingExecutionDirectory = s"executions/$organizationId/$datasetId/$mappingExecutionId";
    logger.info(s"mappingExecutionDirectory = $mappingExecutionDirectory");

    val outputFileName = if (pOutputFilename == null) {
      UUID.randomUUID.toString + ".txt"
    } else {
      pOutputFilename;
    }
    val outputFilepath:String = s"$mappingExecutionDirectory/$outputFileName"
    logger.info(s"outputFilepath = $outputFilepath");
    val outputFile: File = new File(outputFilepath)

    //EXECUTING MAPPING
    try {
      if (MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {
        if(datasetDistribution.dcatMediaType == null) {
          datasetDistribution.dcatMediaType = "text/csv"
        }
        logger.info(s"datasetDistribution.dcatMediaType = ${datasetDistribution.dcatMediaType}")

        if("text/csv".equalsIgnoreCase(datasetDistribution.dcatMediaType)) {
          MappingExecutionController.executeR2RMLMappingWithCSV(md, dataset, outputFilepath, queryFileName);
        } else {
          MappingExecutionController.executeR2RMLMappingWithRDB(md, dataset, outputFilepath, queryFileName, jdbcConnection);
        }
      } else if (MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
        MappingExecutionController.executeRMLMapping(md, dataset, outputFilepath);
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
    val githubResponse = if(MappingPediaEngine.mappingpediaProperties.githubEnabled && pStoreToGithub) {
      try {
        val response = githubClient.encodeAndPutFile(outputFilepath
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
    } else {
      null
    }


    val mappingExecutionResultURL = if(githubResponse != null) {
      if (HttpURLConnection.HTTP_CREATED == githubResponse.getStatus || HttpURLConnection.HTTP_OK == githubResponse.getStatus) {
        githubResponse.getBody.getObject.getJSONObject("content").getString("url");
      } else {
        errorOccured = true;
        val errorMessage = "Error storing mapping execution result on GitHub: " + githubResponse.getStatus
        logger.error(errorMessage)
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        null
      }
    } else {
      null
    }

    val mappingExecutionResultDownloadURL = if(mappingExecutionResultURL != null) {
      try {
        val response = Unirest.get(mappingExecutionResultURL).asJson();
        response.getBody.getObject.getString("download_url");
      } catch {
        case e:Exception => mappingExecutionResultURL
      }
    } else {
      null
    }

    logger.info(s"mappingExecutionResultDownloadURL = $mappingExecutionResultDownloadURL")


    val mappingExecutionResultDistribution = new Distribution(dataset)
    //STORING MAPPING EXECUTION RESULT AS A RESOURCE ON CKAN
    val ckanAddResourceResponse = try {
      if(MappingPediaEngine.mappingpediaProperties.ckanEnable && pStoreToCKAN) {
        logger.info("storing mapping execution result on CKAN ...")
        mappingExecutionResultDistribution.dcatAccessURL = mappingExecutionResultURL;
        mappingExecutionResultDistribution.dcatDownloadURL = mappingExecutionResultDownloadURL;
        mappingExecutionResultDistribution.dcatMediaType = null //TODO FIXME
        mappingExecutionResultDistribution.dctDescription = "Annotated Dataset using the annotation: " + mdDownloadURL;
        mappingExecutionResultDistribution.distributionFile = outputFile;
        //val addNewResourceResponse = CKANUtility.addNewResource(resourceIdentifier, resourceTitle
        //            , resourceMediaType, resourceFileRef, resourceDownloadURL)
        //val addNewResourceResponse = CKANUtility.addNewResource(distribution);
        ckanClient.createResource(mappingExecutionResultDistribution);

        /*
        mappingExecutionResultDistribution.ckanResourceId = addNewResourceEntity.getJSONObject("result").getString("id");
        mappingExecutionResultDistribution.dctTitle = s"Annotated Dataset ${mappingExecutionResultDistribution.dctIdentifier}"
        addNewResourceStatus.getStatusCode
        */
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
    val ckanAddResourceResponseStatusCode:Integer = {
      if(ckanAddResourceResponse == null) {
        null
      } else {
        ckanAddResourceResponse.getStatusLine.getStatusCode
      }
    }

    val manifestFile = this.generateManifestFile(mappingExecutionResultDistribution, datasetDistribution, md)

    //STORING MANIFEST ON GITHUB
    val addManifestFileGitHubResponse:HttpResponse[JsonNode] =
      if(MappingPediaEngine.mappingpediaProperties.githubEnabled && pStoreToGithub) {
        try {
          this.storeManifestFileOnGitHub(manifestFile, dataset, md);
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
      } else {
        null
      }


    //STORING MANIFEST FILE AS TRIPLES ON VIRTUOSO
    val addManifestVirtuosoResponse:String = try {
      if(MappingPediaEngine.mappingpediaProperties.virtuosoEnabled && pStoreToVirtuoso) {
        MappingExecutionController.storeManifestOnVirtuoso(manifestFile);
      } else {
        "Storing to Virtuoso is not enabled!";
      }
    } catch {
      case e: Exception => {
        errorOccured = true;
        e.printStackTrace()
        val errorMessage = "error storing manifest file of a mapping execution result on Virtuoso: " + e.getMessage
        val manifestFileInString = scala.io.Source.fromFile(manifestFile).getLines.reduceLeft(_+_)
        logger.error(errorMessage);
        logger.error(s"manifestFileInString = $manifestFileInString");
        collectiveErrorMessage = errorMessage :: collectiveErrorMessage
        e.getMessage
      }
    }

    val manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse);
    val manifestDownloadURL = this.githubClient.getDownloadURL(manifestAccessURL)

    val (responseStatus, responseStatusText) = if(errorOccured) {
      (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
    } else {
      (HttpURLConnection.HTTP_OK, "OK")
    }


    new ExecuteMappingResult(
      responseStatus, responseStatusText
      , datasetDistributionDownloadURL
      , mdDownloadURL
      , queryFileName
      , mappingExecutionResultURL, mappingExecutionResultDownloadURL
      , ckanAddResourceResponseStatusCode
      , mappingExecutionResultDistribution.dctIdentifier
      , manifestAccessURL, manifestDownloadURL
    )

    /*
    new GeneralResult(null
      , distributionDownloadURL, mdDownloadURL, queryFileName
      , mappingExecutionResultURL, responseStatusText, responseStatus, ckanResponseText)
      */

  }

  def getInstances(aClass:String, jenaClient:JenaClient) : ListResult = {
    /*
    val subclassesListResult = MappingPediaUtility.getSubclassesDetail(
      aClass, MappingPediaEngine.ontologyModel, outputType, inputType);
    logger.info(s"subclassesListResult = subclassesListResult")

    val subclassesURIs:Iterable[String] = subclassesListResult.results.map(
      result => result.asInstanceOf[OntologyClass].getURI).toList.distinct
    //		val subclassesInList:Iterable[String] = subclassesListResult.results.values.map(
    //      result => result.asInstanceOf[OntologyClass].aClass).toList.distinct

    logger.debug("subclassesInList" + subclassesURIs)
    //new ListResult(subclassesInList.size, subclassesInList);
    val queryFile:String = null;

    val mappingDocuments = subclassesURIs.flatMap(subclassURI => {
        MappingDocumentController.findMappingDocumentsByMappedClass(subclassURI).getResults();
    }).asInstanceOf[Iterable[MappingDocument]];
    */

    val mappingDocuments = MappingDocumentController.findMappingDocumentsByMappedSubClass(aClass, jenaClient).results

    var executedMappingDocuments:List[(String, String)]= Nil;

    val executionResults:Iterable[ExecutionMappingResultSummary] = mappingDocuments.flatMap(mappingDocument => {
      val md = mappingDocument.asInstanceOf[MappingDocument];

      val mappingLanguage = md.mappingLanguage;
      val distributionFieldSeparator = if(md.distributionFieldSeparator != null && md.distributionFieldSeparator.isDefined) {
        md.distributionFieldSeparator.get
      } else {
        null
      }
      val outputFilename = UUID.randomUUID.toString + ".nt"
      //val mappingDocumentDownloadURL = md.getDownloadURL();

      val dataset = new Dataset(new Organization());
      val distribution = new Distribution(dataset);
      dataset.addDistribution(distribution);
      distribution.dcatDownloadURL = md.dataset.getDistribution().dcatDownloadURL;
      distribution.sha = md.dataset.getDistribution().sha


      if(md.sha != null && distribution.sha != null) {
        logger.info(s"mdSHA = ${md.sha}");
        logger.info(s"mdDistributionSHA = ${distribution.sha}");

        if(executedMappingDocuments.contains((md.sha,distribution.sha))) {
          None
        } else {


          val mappingExecution = new MappingExecution(md, dataset);
          mappingExecution.setStoreToCKAN("false")
          mappingExecution.queryFilePath = null;
          mappingExecution.outputFileName = outputFilename;

          //THERE IS NO NEED TO STORE THE EXECUTION RESULT IN THIS PARTICULAR CASE
          val executionResult = this.executeMapping(md, dataset, mappingExecution.queryFilePath, outputFilename
            , false, true, false
            , null
          );
          //val executionResult = MappingExecutionController.executeMapping2(mappingExecution);

          executedMappingDocuments = (md.sha,distribution.sha) :: executedMappingDocuments;

          val executionResultAccessURL = executionResult.getMapping_execution_result_access_url()
          val executionResultDownloadURL = executionResult.getMapping_execution_result_download_url
          //executionResultURL;

          Some(new ExecutionMappingResultSummary(md, distribution, executionResultAccessURL, executionResultDownloadURL))
          //mappingDocumentURL + " -- " + datasetDistributionURL
        }
      } else {
        None
      }


    })
    new ListResult(executionResults.size, executionResults);

  }

  def generateManifestFile(mappingExecutionResult:Distribution, datasetDistribution: Distribution
                           , mappingDocument:MappingDocument) = {

    val dataset = datasetDistribution.dataset;

    logger.info("Generating manifest file for Mapping Execution Result ...")
    try {
      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-mappingexecutionresult-template.ttl"
      );

      val datasetDistributionDownloadURL:String = s"<${datasetDistribution.dcatDownloadURL}>";
      logger.info(s"datasetDistributionDownloadURL = ${datasetDistributionDownloadURL}")

      val mapValues:Map[String,String] = Map(
        "$mappingExecutionResultID" -> mappingExecutionResult.dctIdentifier
        , "$mappingExecutionResultTitle" -> mappingExecutionResult.dctTitle
        , "$mappingExecutionResultDescription" -> mappingExecutionResult.dctDescription
        , "$datasetDistributionDownloadURL" -> datasetDistributionDownloadURL
        , "$mappingDocumentID" -> mappingDocument.dctIdentifier
      );

      val filename = s"metadata-mappingexecutionresult-${mappingExecutionResult.dctIdentifier}.ttl";
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

  def storeManifestFileOnGitHub(manifestFile:File, dataset:Dataset, mappingDocument: MappingDocument) = {
    val organization = dataset.dctPublisher;

    logger.info("storing manifest file on github ...")
    val addNewManifestCommitMessage = s"Add manifest file for the execution of mapping document: ${mappingDocument.dctIdentifier}"
    val githubResponse = githubClient.encodeAndPutFile(organization.dctIdentifier
      , dataset.dctIdentifier, manifestFile.getName, addNewManifestCommitMessage, manifestFile)
    logger.info("manifest file stored on github ...")
    githubResponse
  }

}

object MappingExecutionController {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  /*
  val ckanUtility = new CKANUtility(
    MappingPediaEngine.mappingpediaProperties.ckanURL, MappingPediaEngine.mappingpediaProperties.ckanKey)
  val githubClient = MappingPediaEngine.githubClient;
  */


  def executeR2RMLMappingWithRDB(md:MappingDocument, dataset: Dataset
                                 , outputFilepath:String, queryFileName:String
                                /*
                                , dbUserName:String, dbPassword:String
                                , dbName:String, jdbc_url:String
                                , databaseDriver:String, databaseType:String
                                */
                                , jdbcConnection: JDBCConnection

                                ) = {
    logger.info("Executing R2RML mapping ...")
    val distribution = dataset.getDistribution();
    val randomUUID = UUID.randomUUID.toString
    val databaseName =  s"executions/${md.dctIdentifier}/${randomUUID}"
    logger.info(s"databaseName = $databaseName")

    val properties: MorphRDBProperties = new MorphRDBProperties
    properties.setNoOfDatabase(1)
    properties.setDatabaseUser(jdbcConnection.dbUserName)
    properties.setDatabasePassword(jdbcConnection.dbPassword)
    properties.setDatabaseName(jdbcConnection.dbName)
    properties.setDatabaseURL(jdbcConnection.jdbc_url)
    properties.setDatabaseDriver(jdbcConnection.databaseDriver)
    properties.setDatabaseType(jdbcConnection.databaseType)
    properties.setMappingDocumentFilePath(md.getDownloadURL())
    properties.setOutputFilePath(outputFilepath)


    val runnerFactory: MorphRDBRunnerFactory = new MorphRDBRunnerFactory
    val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
    runner.run
  }

  def executeR2RMLMappingWithCSV(md:MappingDocument, dataset: Dataset, outputFilepath:String, queryFileName:String) = {
    logger.info("Executing R2RML mapping ...")
    val mappingDocumentDownloadURL = md.getDownloadURL();
    logger.info(s"mappingDocumentDownloadURL = $mappingDocumentDownloadURL");

    val distribution = dataset.getDistribution();
    val datasetDistributionDownloadURL = distribution.dcatDownloadURL;
    logger.info(s"datasetDistributionDownloadURL = $datasetDistributionDownloadURL");


    val randomUUID = UUID.randomUUID.toString
    val databaseName =  s"executions/${md.dctIdentifier}/${randomUUID}"
    logger.info(s"databaseName = $databaseName")

    val properties: MorphCSVProperties = new MorphCSVProperties
    properties.setDatabaseName(databaseName)
    properties.setMappingDocumentFilePath(mappingDocumentDownloadURL)
    properties.setOutputFilePath(outputFilepath);
    properties.setCSVFile(datasetDistributionDownloadURL);
    properties.setQueryFilePath(queryFileName);
    if (distribution.cvsFieldSeparator != null) {
      properties.fieldSeparator = Some(distribution.cvsFieldSeparator);
    }

    val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
    val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
    runner.run
  }

  def executeRMLMapping(md:MappingDocument, dataset: Dataset, outputFilepath:String) = {
    logger.info("Executing RML mapping ...")

    val datasetDistributionDownloadURL = dataset.getDistribution().dcatDownloadURL;
    logger.info(s"datasetDistributionDownloadURL = $datasetDistributionDownloadURL")

    val mdDownloadURL = md.getDownloadURL();
    logger.info(s"mdDownloadURL = $mdDownloadURL")

    val rmlConnector = new RMLMapperConnector();
    rmlConnector.executeWithMain(datasetDistributionDownloadURL, mdDownloadURL, outputFilepath);
  }

  def storeManifestOnVirtuoso(manifestFile:File) = {
    if(manifestFile != null) {
      logger.info("storing the manifest triples of a mapping execution result on virtuoso ...")
      logger.debug("manifestFile = " + manifestFile);
      MappingPediaEngine.virtuosoClient.store(manifestFile)
      logger.info("manifest triples stored on virtuoso.")
      "OK";
    } else {
      "No manifest file specified/generated!";
    }
  }
}

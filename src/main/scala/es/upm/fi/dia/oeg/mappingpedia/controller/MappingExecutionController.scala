package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.{Date, UUID}

import com.mashape.unirest.http.{HttpResponse, JsonNode, Unirest}
import es.upm.fi.dia.oeg.mappingpedia
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.{logger, sdf}
import es.upm.fi.dia.oeg.mappingpedia.model.result.{ExecuteMappingResult, ExecuteMappingResultSummary, GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController.logger
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController.logger
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility._
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory, MorphRDBProperties, MorphRDBRunnerFactory}

import scala.collection.JavaConversions._

class MappingExecutionController(val ckanClient:CKANUtility
                                 , val githubClient:GitHubUtility
                                 , val virtuosoClient: VirtuosoClient
                                 , val jenaClient:JenaClient) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val mappingDocumentController:MappingDocumentController = new MappingDocumentController(githubClient, virtuosoClient, jenaClient);

  def findMappingExecutionURLBySHA(mdSHA:String, datasetDistributionSHA:String) = {
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mdSHA" -> mdSHA
      , "$datasetDistributionSHA" -> datasetDistributionSHA
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findMappingExecutionResultBySHA.rq");
    logger.info(s"queryString = ${queryString}");

    val qexec = this.virtuosoClient.createQueryExecution(queryString);

    var results: List[String] = List.empty;
    try {
      val rs = qexec.execSelect
      while (rs.hasNext) {
        val qs = rs.nextSolution
        val mappedClass = qs.get("downloadURL").toString;
        results = mappedClass :: results;
      }
    } finally qexec.close

    val listResult = new ListResult(results.length, results);


    listResult
  }

  @throws(classOf[Exception])
  def executeMapping(
                      md:MappingDocument
                      , dataset:Dataset
                      , queryFileName:String
                      , pOutputFilename: String

                      , pStoreToCKAN:Boolean
                      , pStoreToGithub:Boolean
                      , pStoreExecutionResultToVirtuoso:Boolean

                      , jdbcConnection: JDBCConnection

                      , useCache:Boolean
                    ) : ExecuteMappingResult = {
    var errorOccured = false;
    var collectiveErrorMessage: List[String] = Nil;

    val organization = dataset.dctPublisher;
    val datasetDistribution = dataset.getDistribution();
    val datasetDistributionDownloadURL = datasetDistribution.dcatDownloadURL;
    val datasetDistributionSHA = datasetDistribution.sha;
    if (datasetDistribution.sha == null && datasetDistribution.dcatDownloadURL != null ) {
      val hashValue = MappingPediaUtility.calculateHash(datasetDistribution.dcatDownloadURL, datasetDistribution.encoding);
      datasetDistribution.sha = hashValue
    }

    val mdDownloadURL = md.getDownloadURL();
    val mdSHA = md.sha;
    if (md.sha == null && mdDownloadURL != null ) {
      val hashValue = MappingPediaUtility.calculateHash(mdDownloadURL, "UTF-8");
      datasetDistribution.sha = hashValue
    }

    val cacheExecutionURL = this.findMappingExecutionURLBySHA(mdSHA, datasetDistributionSHA);
    logger.info(s"cacheExecutionURL = ${cacheExecutionURL}");

    if(cacheExecutionURL == null || cacheExecutionURL.results.isEmpty) {
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

      val outputFileName = if (pOutputFilename == null) {
        UUID.randomUUID.toString + ".txt"
      } else {
        pOutputFilename;
      }
      val outputFilepath:String = s"$mappingExecutionDirectory/$outputFileName"
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
      mappingExecutionResultDistribution.dcatAccessURL = mappingExecutionResultURL;
      mappingExecutionResultDistribution.dcatDownloadURL = mappingExecutionResultDownloadURL;
      mappingExecutionResultDistribution.dcatMediaType = null //TODO FIXME
      mappingExecutionResultDistribution.dctDescription = "Annotated Dataset using the annotation: " + mdDownloadURL;
      mappingExecutionResultDistribution.distributionFile = outputFile;

      //GENERATING MANIFEST FILE
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
      val manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse);
      val manifestDownloadURL = this.githubClient.getDownloadURL(manifestAccessURL)

      val mappedClass:String = try {
        this.mappingDocumentController.findAllMappedClassesByMappingDocumentId(md.dctIdentifier).results.iterator.next().toString
      } catch {
        case e:Exception => null
      }
      logger.info(s"mappedClass = $mappedClass")

      //STORING MAPPING EXECUTION RESULT AS A RESOURCE ON CKAN
      val ckanAddResourceResponse = try {
        if(MappingPediaEngine.mappingpediaProperties.ckanEnable && pStoreToCKAN) {
          logger.info("storing mapping execution result on CKAN ...")


          //val addNewResourceResponse = CKANUtility.addNewResource(resourceIdentifier, resourceTitle
          //            , resourceMediaType, resourceFileRef, resourceDownloadURL)
          //val addNewResourceResponse = CKANUtility.addNewResource(distribution);

          val mapTextBody:Map[String, String] = Map(
            MappingPediaConstant.CKAN_RESOURCE_ORIGINAL_DATASET_DISTRIBUTION_DOWNLOAD_URL -> datasetDistribution.dcatDownloadURL
            , MappingPediaConstant.CKAN_RESOURCE_MAPPING_DOCUMENT_DOWNLOAD_URL -> md.getDownloadURL()
            , MappingPediaConstant.CKAN_RESOURCE_PROV_TRIPLES -> manifestDownloadURL
            , MappingPediaConstant.CKAN_RESOURCE_CLASS -> mappedClass
          )
          ckanClient.createResource(mappingExecutionResultDistribution, Some(mapTextBody));

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






      //STORING MANIFEST FILE AS TRIPLES ON VIRTUOSO
      val addManifestVirtuosoResponse:String = try {
        if(MappingPediaEngine.mappingpediaProperties.virtuosoEnabled) {
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


      val (responseStatus, responseStatusText) = if(errorOccured) {
        (HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error: " + collectiveErrorMessage.mkString("[", ",", "]"))
      } else {
        (HttpURLConnection.HTTP_OK, "OK")
      }

      logger.info("\n")

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
    } else {
      val mappingExecutionResultURL = cacheExecutionURL.results.iterator.next().toString;

      new ExecuteMappingResult(
        HttpURLConnection.HTTP_OK, "OK"
        , datasetDistributionDownloadURL
        , mdDownloadURL
        , queryFileName
        , mappingExecutionResultURL, mappingExecutionResultURL
        , null
        , null
        , null, null
      )
    }



    /*
    new GeneralResult(null
      , distributionDownloadURL, mdDownloadURL, queryFileName
      , mappingExecutionResultURL, responseStatusText, responseStatus, ckanResponseText)
      */

  }

  def getInstances(aClass:String, maxMappingDocuments:Integer, useCache:Boolean) : ListResult = {
    logger.info(s"useCache = ${useCache}");

    val mappingDocuments =
      this.mappingDocumentController.findMappingDocumentsByMappedClassAndProperty(
        aClass, null, true).results

    var executedMappingDocuments:List[(String, String)]= Nil;

    var i = 0;
    val executionResults:Iterable[ExecuteMappingResultSummary] = mappingDocuments.flatMap(
      mappingDocument => { val md = mappingDocument.asInstanceOf[MappingDocument];

        val mappingLanguage = md.mappingLanguage;
        val distributionFieldSeparator = if(md.distributionFieldSeparator != null
          && md.distributionFieldSeparator.isDefined) {
          md.distributionFieldSeparator.get
        } else {
          null
        }
        val outputFilename = UUID.randomUUID.toString + ".nt"
        //val mappingDocumentDownloadURL = md.getDownloadURL();

        val dataset = new Dataset(new Agent());
        val distribution = new Distribution(dataset);
        dataset.addDistribution(distribution);
        distribution.dcatDownloadURL = md.dataset.getDistribution().dcatDownloadURL;
        distribution.sha = md.dataset.getDistribution().sha
        if (distribution.sha == null && distribution.dcatDownloadURL != null ) {
          val hashValue = MappingPediaUtility.calculateHash(distribution.dcatDownloadURL, distribution.encoding);
          distribution.sha = hashValue
        }


        logger.info(s"mapping document SHA = ${md.sha}");
        logger.info(s"dataset distribution SHA = ${distribution.sha}");

        if(executedMappingDocuments.contains((md.sha,distribution.sha))) {
          None
        } else {
          if(i < maxMappingDocuments) {
            val mappingExecutionURLs = if(useCache) { this.findMappingExecutionURLBySHA(md.sha,distribution.sha); }
            else { null }

            if(useCache && mappingExecutionURLs != null && mappingExecutionURLs.results.size > 0) {
              val cachedMappingExecutionResultURL:String = mappingExecutionURLs.results.iterator.next().toString;
              logger.info(s"cachedMappingExecutionResultURL = " + cachedMappingExecutionResultURL)

              i +=1
              Some(new ExecuteMappingResultSummary(md, distribution
                , cachedMappingExecutionResultURL, cachedMappingExecutionResultURL))
            } else {
              val mappingExecution = new MappingExecution(md, dataset);
              mappingExecution.setStoreToCKAN("false")
              mappingExecution.queryFilePath = null;
              mappingExecution.outputFileName = outputFilename;

              //THERE IS NO NEED TO STORE THE EXECUTION RESULT IN THIS PARTICULAR CASE
              val executionResult = this.executeMapping(md, dataset, mappingExecution.queryFilePath, outputFilename
                , false, true, false
                , null
                , useCache
              );
              //val executionResult = MappingExecutionController.executeMapping2(mappingExecution);

              executedMappingDocuments = (md.sha,distribution.sha) :: executedMappingDocuments;

              val executionResultAccessURL = executionResult.getMapping_execution_result_access_url()
              val executionResultDownloadURL = executionResult.getMapping_execution_result_download_url
              //executionResultURL;

              i +=1
              Some(new ExecuteMappingResultSummary(md, distribution, executionResultAccessURL, executionResultDownloadURL))
              //mappingDocumentURL + " -- " + datasetDistributionURL
            }

          } else {
            None
          }
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

      val datasetDistributionDownloadURL:String = if( datasetDistribution.dcatDownloadURL == null) { ""}
      else { datasetDistribution.dcatDownloadURL }
      logger.info(s"datasetDistributionDownloadURL = ${datasetDistributionDownloadURL}")

      val downloadURL = if(mappingExecutionResult.dcatDownloadURL == null) { "" }
      else { mappingExecutionResult.dcatDownloadURL }
      logger.info(s"downloadURL = ${downloadURL}")

      val mappingDocumentSHA = if(mappingDocument.sha == null) { "" } else { mappingDocument.sha }
      logger.info(s"mappingDocumentSHA = ${mappingDocumentSHA}")
      val datasetDistributionSHA = if(datasetDistribution.sha == null) { ""} else {datasetDistribution.sha}
      logger.info(s"datasetDistributionSHA = ${datasetDistributionSHA}")

      val mapValues:Map[String,String] = Map(
        "$mappingExecutionResultID" -> mappingExecutionResult.dctIdentifier
        , "$mappingExecutionResultTitle" -> mappingExecutionResult.dctTitle
        , "$mappingExecutionResultDescription" -> mappingExecutionResult.dctDescription
        , "$datasetDistributionDownloadURL" -> datasetDistributionDownloadURL
        , "$mappingDocumentID" -> mappingDocument.dctIdentifier
        , "$downloadURL" -> downloadURL
        , "$mappingDocumentSHA" -> mappingDocumentSHA
        , "$datasetDistributionSHA" -> datasetDistributionSHA
        , "$issued" -> mappingExecutionResult.dctIssued
        , "$modified" -> mappingExecutionResult.dctModified

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

  def getMappingExecutionResultURL(mdSHA:String, datasetDistributionSHA:String) = {

  }
}

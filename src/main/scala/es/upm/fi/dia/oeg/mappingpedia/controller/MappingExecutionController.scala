package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.{ByteArrayOutputStream, File, InputStream}
import java.net.{HttpURLConnection, URL}
import java.util.{Date, UUID}

import com.mashape.unirest.http.{HttpResponse, JsonNode, Unirest}
import es.upm.fi.dia.oeg.mappingpedia.model.result.{ExecuteMappingResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility._
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory, MorphRDBProperties, MorphRDBRunnerFactory}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.impl.client.CloseableHttpClient
import org.json.JSONObject
import org.springframework.http.HttpStatus

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class MappingExecutionController(val ckanClient:CKANUtility
                                 , val githubClient:GitHubUtility
                                 , val virtuosoClient: VirtuosoClient
                                 , val jenaClient:JenaClient
                                )
{
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  val mappingDocumentController:MappingDocumentController = new MappingDocumentController(
    ckanClient, githubClient, virtuosoClient, jenaClient);
  //val mapper = new ObjectMapper();

  val helper = new MappingExecutionControllerHelper(this);
  val helperThread = new Thread(helper);
  helperThread.start();



  def findByHash(mdHash:String, datasetDistributionHash:String) = {
    val mapValues: Map[String, String] = Map(
      "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
      , "$mdHash" -> mdHash
      , "$datasetDistributionHash" -> datasetDistributionHash
    );

    val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
      mapValues, "templates/findMappingExecutionResultByHash.rq");
    logger.debug(s"queryString = ${queryString}");

    var results: List[String] = List.empty;

    if(this.virtuosoClient != null) {
      val qexec = this.virtuosoClient.createQueryExecution(queryString);
      try {
        val rs = qexec.execSelect
        while (rs.hasNext) {
          val qs = rs.nextSolution
          val mappedClass = qs.get("downloadURL").toString;
          results = mappedClass :: results;
        }
      } finally qexec.close
    }

    new ListResult(results.length, results);
  }


  @throws(classOf[Exception])
  def executeMapping(
                      mappingExecution: MappingExecution
                    ) : ExecuteMappingResult = {
    val mapper = new ObjectMapper();
    val callbackURL = mappingExecution.callbackURL;

    val executeMappingResult = if(callbackURL != null) {
      this.helper.executionQueue.enqueue(mappingExecution);
      new ExecuteMappingResult(
        HttpURLConnection.HTTP_ACCEPTED, HttpStatus.ACCEPTED.getReasonPhrase
        , mappingExecution
        , null
      )
    } else {
      var result:ExecuteMappingResult = null;
      while(result == null) {
        if(!this.helper.isProcessing && this.helper.executionQueue.size==0) {
          this.helper.isProcessing = true;
          val f = this.executeMappingWithFuture(mappingExecution);
          logger.info("Await.result");
          result = Await.result(f, 60 second)
          this.helper.isProcessing = false;
        } else {
          Thread.sleep(1000) // wait for 1000 millisecond
        }
      }

      result;
    }



    //MappingExecutionController.executionQueue.enqueue(mappingExecution);


    /*    val f = this.executeMappingWithFuture(mappingExecution);
        val mapper = new ObjectMapper();
        val callbackURL = mappingExecution.callbackURL
        val executeMappingResult = if(callbackURL == null) {
          logger.info("Await.result");
          val result = Await.result(f, 60 second)
          MappingExecutionController.executionQueue.dequeue()
          result;
        } else {
          f.onComplete {
            case Success(forkExecuteMappingResult:ExecuteMappingResult) => {
              logger.info("f.onComplete Success");

              val forkExecuteMappingResultAsString = mapper.writeValueAsString(forkExecuteMappingResult)
              logger.info(s"forkExecuteMappingResultAsString = ${forkExecuteMappingResultAsString}");

              val manifestFile = forkExecuteMappingResult.getManifest_download_url;
              val jsonObj = if(manifestFile == null ) {
                val annotatedDistributionURL = forkExecuteMappingResult.getMapping_execution_result_download_url;
                logger.debug(s"annotatedDistributionURL = ${annotatedDistributionURL}");

                val newJsonObj = new JSONObject();
                newJsonObj.put("@id", forkExecuteMappingResult.mappingExecutionResult.dctIdentifier);
                newJsonObj.put("downloadURL", annotatedDistributionURL);

                val context = new JSONObject();
                newJsonObj.put("@context", context);

                val downloadURLContext = new JSONObject();
                context.put("downloadURL", downloadURLContext);

                downloadURLContext.put("type", "@id")
                downloadURLContext.put("@id", "http://www.w3.org/ns/dcat#downloadURL")

                newJsonObj
              } else {
                val manifestStringJsonLd = JenaClient.urlToString(manifestFile, Some("TURTLE"));
                val jsonObjFromManifest = new JSONObject(manifestStringJsonLd);
                jsonObjFromManifest
              }

              val response = Unirest.post(callbackURL)
                .header("Content-Type", "application/json")
                .body(jsonObj)
                .asString();
              logger.info(s"POST to ${callbackURL} with body = ${jsonObj.toString(3)}")

              try {
                logger.info(s"response from callback = ${response.getBody}")
              } catch {
                case e:Exception => {
                  e.printStackTrace()
                }
              }

              MappingExecutionController.executionQueue.dequeue()
            }
            case Failure(e) => {
              logger.info("f.onComplete Success Failure");
              e.printStackTrace
              MappingExecutionController.executionQueue.dequeue()
            }
          }


          logger.info("In Progress");
          new ExecuteMappingResult(
            HttpURLConnection.HTTP_ACCEPTED, HttpStatus.ACCEPTED.getReasonPhrase
            , mappingExecution
            , null
          )
        }*/



    try {
      val executeMappingResultAsString = mapper.writeValueAsString(executeMappingResult);
      logger.info(s"executeMappingResult = ${executeMappingResult}");
    } catch {
      case e:Exception => {
        logger.error(s"executeMappingResult = ${executeMappingResult}")
      }
    }

    executeMappingResult
  }

  @throws(classOf[Exception])
  def executeMappingWithFuture(
                                mappingExecution: MappingExecution
                              ) : Future[ExecuteMappingResult] = {
    val pStoreToGithub = mappingExecution.pStoreToGithub
    val useCache = mappingExecution.useCache
    val pStoreToCKAN = mappingExecution.storeToCKAN;

    val f = Future {
      var errorOccured = false;
      var collectiveErrorMessage: List[String] = Nil;

      val md = mappingExecution.mappingDocument
      val unannotatedDistributions = mappingExecution.unannotatedDistributions
      val dataset = unannotatedDistributions.iterator.next().dataset;

      val organization = dataset.dctPublisher
      //val unannotatedDistributions = dataset.getUnannotatedDistributions
      val unannotatedDatasetHash = MappingPediaUtility.calculateHash(
        unannotatedDistributions);

      val mdDownloadURL = md.getDownloadURL();
      if (md.hash == null && mdDownloadURL != null ) {
        val hashValue = MappingPediaUtility.calculateHash(mdDownloadURL, "UTF-8");
        md.hash = hashValue
      }

      val cacheExecutionURL = this.findByHash(md.hash, unannotatedDatasetHash);
      logger.debug(s"cacheExecutionURL = ${cacheExecutionURL}");

      if(cacheExecutionURL == null || cacheExecutionURL.results.isEmpty || !useCache) {


        val mappedClasses:String = try {
          this.mappingDocumentController.findMappedClassesByMappingDocumentId(
            md.dctIdentifier).results.mkString(",");
        } catch { case e:Exception => null }
        logger.info(s"mappedClasses = $mappedClasses")

        val organizationId = if(organization != null) { organization.dctIdentifier }
        else { UUID.randomUUID.toString }

        val datasetId = dataset.dctIdentifier

        val mappingExecutionId = UUID.randomUUID.toString;
        val annotatedDistribution = new AnnotatedDistribution(dataset, mappingExecutionId)
        annotatedDistribution.dcatMediaType = mappingExecution.pOutputMediaType
        annotatedDistribution.dctDescription = "Annotated Dataset using the annotation: " + mdDownloadURL;


        val mappingExecutionDirectory = s"executions/$organizationId/$datasetId/${md.dctIdentifier}";

        val outputFileNameWithExtension:String = mappingExecution.getOutputFileWithExtension;

        val githubOutputFilepath:String = s"$mappingExecutionDirectory/$outputFileNameWithExtension"
        val localOutputDirectory = s"$mappingExecutionDirectory/$mappingExecutionId";
        mappingExecution.outputDirectory = localOutputDirectory;
        val localOutputFilepath:String = s"$localOutputDirectory/$outputFileNameWithExtension"
        val localOutputFile: File = new File(localOutputFilepath)
        annotatedDistribution.distributionFile = localOutputFile;

        //EXECUTING MAPPING
        try {
          MappingExecutionController.executeMapping(mappingExecution);
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
            val response = githubClient.encodeAndPutFile(githubOutputFilepath
              , "add mapping execution result by mappingpedia engine", localOutputFile);
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
        annotatedDistribution.dcatAccessURL = mappingExecutionResultURL;

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
        annotatedDistribution.dcatDownloadURL = mappingExecutionResultDownloadURL;



        //STORING MAPPING EXECUTION RESULT AS A RESOURCE ON CKAN
        val ckanAddResourceResponse = try {
          if(MappingPediaEngine.mappingpediaProperties.ckanEnable && pStoreToCKAN) {

            val annotatedResourcesIds = ckanClient.getAnnotatedResourcesIds(dataset.ckanPackageId);
            logger.info(s"annotatedResourcesIds = ${annotatedResourcesIds}");

            logger.info("STORING MAPPING EXECUTION RESULT ON CKAN ...")
            //val unannotatedDistributionsDownloadURLs = unannotatedDistributions.map(distribution => distribution.dcatDownloadURL);
            val mapTextBody:Map[String, String] = Map(
              MappingPediaConstant.CKAN_RESOURCE_ORIGINAL_DATASET_DISTRIBUTION_DOWNLOAD_URL ->
                unannotatedDistributions.map(distribution => distribution.dcatDownloadURL).mkString(",")
              , MappingPediaConstant.CKAN_RESOURCE_MAPPING_DOCUMENT_DOWNLOAD_URL -> md.getDownloadURL()
              //, MappingPediaConstant.CKAN_RESOURCE_PROV_TRIPLES -> annotatedDistribution.manifestDownloadURL
              , MappingPediaConstant.CKAN_RESOURCE_CLASS -> mappedClasses
              //, "$manifestDownloadURL" -> annotatedDistribution.manifestDownloadURL
              //, MappingPediaConstant.CKAN_RESOURCE_CLASSES -> mappedClasses
              , MappingPediaConstant.CKAN_RESOURCE_IS_ANNOTATED -> "true"
              , MappingPediaConstant.CKAN_RESOURCE_ORIGINAL_DISTRIBUTION_CKAN_ID ->
                unannotatedDistributions.map(distribution => distribution.ckanResourceId).mkString(",")
            )

            if(annotatedResourcesIds != null && annotatedResourcesIds.size >= 1 && mappingExecution.updateResource) {
              val updateStatusList = annotatedResourcesIds.map(annotatedResourceId => {
                annotatedDistribution.ckanResourceId = annotatedResourceId;
                ckanClient.updateResource(annotatedDistribution, Some(mapTextBody));
              })

              val updateStatus = updateStatusList.iterator.next();
              updateStatus
            } else {
              val createStatus = ckanClient.createResource(annotatedDistribution, Some(mapTextBody));
              createStatus
            }

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
          if(ckanAddResourceResponse == null) { null }
          else { ckanAddResourceResponse.getStatusLine.getStatusCode }
        }

        if(ckanAddResourceResponseStatusCode != null && ckanAddResourceResponseStatusCode >= 200
          && ckanAddResourceResponseStatusCode <300) {
          try {
            val ckanAddResourceResult = CKANUtility.getResult(ckanAddResourceResponse);
            val packageId = ckanAddResourceResult.getString("package_id")
            val resourceId = ckanAddResourceResult.getString("id")
            val resourceURL = ckanAddResourceResult.getString("url")

            annotatedDistribution.dcatAccessURL= s"${this.ckanClient.ckanUrl}/dataset/${packageId}/resource/${resourceId}";
            logger.debug(s"annotatedDistribution.dcatAccessURL = ${annotatedDistribution.dcatAccessURL}")

            annotatedDistribution.dcatDownloadURL = resourceURL;
            logger.debug(s"annotatedDistribution.dcatDownloadURL = ${annotatedDistribution.dcatDownloadURL}")
          } catch { case e:Exception => { e.printStackTrace() } }
        }



        //GENERATING MANIFEST FILE
        val manifestFile = MappingExecutionController.generateManifestFile(
          annotatedDistribution, unannotatedDistributions, md)
        logger.info("Manifest file generated.")


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
        //val manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse);
        annotatedDistribution.manifestAccessURL = this.githubClient.getAccessURL(addManifestFileGitHubResponse);;

        //val manifestDownloadURL = this.githubClient.getDownloadURL(manifestAccessURL)
        annotatedDistribution.manifestDownloadURL = this.githubClient.getDownloadURL(annotatedDistribution.manifestAccessURL);








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
          , mappingExecution
          , annotatedDistribution
        )
      } else {
        val mappingExecutionResultURL = cacheExecutionURL.results.iterator.next().toString;
        val annotatedDistribution = new AnnotatedDistribution(dataset);
        annotatedDistribution.dcatDownloadURL = mappingExecutionResultURL;

        new ExecuteMappingResult(
          HttpURLConnection.HTTP_OK, "OK"
          , mappingExecution
          , annotatedDistribution
        )
      }
    }

    f;

  }

  def getInstances(aClass:String, maxMappingDocuments:Integer, useCache:Boolean, updateResource:Boolean) = {
    logger.info(s"useCache = ${useCache}");

    val mappingDocuments =
      this.mappingDocumentController.findByClassAndProperty(
        aClass, null, true).results

    var executedMappingDocuments:List[(String, String)]= Nil;

    var i = 0;
    val executionResults:Iterable[ExecuteMappingResult] = mappingDocuments.flatMap(
      md => {

        val mappingLanguage = md.mappingLanguage;
        val distributionFieldSeparator = if(md.distributionFieldSeparator != null
          && md.distributionFieldSeparator.isDefined) {
          md.distributionFieldSeparator.get
        } else {
          null
        }
        val outputFileName = UUID.randomUUID.toString
        val outputFileExtension = ".nt";
        //val mappingDocumentDownloadURL = md.getDownloadURL();

        val dataset = new Dataset(new Agent());
        val unannotatedDistribution = new UnannotatedDistribution(dataset);
        val unannotatedDistributions = List(unannotatedDistribution);
        //dataset.addDistribution(unannotatedDistribution);
        unannotatedDistribution.dcatDownloadURL = md.dataset.getDistribution().dcatDownloadURL;
        unannotatedDistribution.hash = md.dataset.getDistribution().hash
        if (unannotatedDistribution.hash == null && unannotatedDistribution.dcatDownloadURL != null ) {
          val hashValue = MappingPediaUtility.calculateHash(unannotatedDistribution.dcatDownloadURL, unannotatedDistribution.encoding);
          unannotatedDistribution.hash = hashValue
        }


        logger.info(s"mapping document SHA = ${md.hash}");
        logger.info(s"dataset distribution hash = ${unannotatedDistribution.hash}");

        if(executedMappingDocuments.contains((md.hash,unannotatedDistribution.hash))) {
          None
        } else {
          if(i < maxMappingDocuments) {
            val jDBCConnection = null;
            val queryFileName = null;
            val outputMediaType = "text/turtle";

            val mappingExecution = new MappingExecution(md, unannotatedDistributions
              , jDBCConnection, queryFileName
              , outputFileName, outputFileExtension, outputMediaType
              , false
              , true
              , false
              , useCache
              , null
              , updateResource
            );

            val mappingExecutionURLs = if(useCache) { this.findByHash(md.hash,unannotatedDistribution.hash); }
            else { null }

            if(useCache && mappingExecutionURLs != null && mappingExecutionURLs.results.size > 0) {
              val cachedMappingExecutionResultURL:String = mappingExecutionURLs.results.iterator.next().toString;
              logger.info(s"cachedMappingExecutionResultURL = " + cachedMappingExecutionResultURL)

              i +=1
              val mappingExecutionResult = new AnnotatedDistribution(dataset);
              mappingExecutionResult.dcatAccessURL = cachedMappingExecutionResultURL;
              mappingExecutionResult.dcatDownloadURL = cachedMappingExecutionResultURL;

              Some(new ExecuteMappingResult(HttpURLConnection.HTTP_OK, "OK"
                , mappingExecution, mappingExecutionResult))
            } else {
              val unannotatedDistributions = dataset.getUnannotatedDistributions;
              val storeToCKAN = MappingPediaUtility.stringToBoolean("false");
              mappingExecution.storeToCKAN = storeToCKAN

              //THERE IS NO NEED TO STORE THE EXECUTION RESULT IN THIS PARTICULAR CASE
              val executionResult = this.executeMapping(mappingExecution);
              //val executionResult = MappingExecutionController.executeMapping2(mappingExecution);

              executedMappingDocuments = (md.hash,unannotatedDistribution.hash) :: executedMappingDocuments;

              val executionResultAccessURL = executionResult.getMapping_execution_result_access_url()
              val executionResultDownloadURL = executionResult.getMapping_execution_result_download_url
              //executionResultURL;

              i +=1
              Some(executionResult);
            }
          } else {
            None
          }
        }



      })
    new ListResult(executionResults.size, executionResults);

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
  //var executionQueue = new mutable.Queue[MappingExecution];



  /*
  val ckanUtility = new CKANUtility(
    MappingPediaEngine.mappingpediaProperties.ckanURL, MappingPediaEngine.mappingpediaProperties.ckanKey)
  val githubClient = MappingPediaEngine.githubClient;
  */


  def executeR2RMLMappingWithRDB(mappingExecution: MappingExecution) = {
    logger.info("Executing R2RML mapping (RDB) ...")
    val md = mappingExecution.mappingDocument;
    val mappingDocumentDownloadURL = md.getDownloadURL();
    logger.info(s"mappingDocumentDownloadURL = $mappingDocumentDownloadURL");

    val outputFilepath = if(mappingExecution.outputDirectory == null) { mappingExecution.getOutputFileWithExtension; }
    else { s"${mappingExecution.outputDirectory}/${mappingExecution.getOutputFileWithExtension}"}
    logger.info(s"outputFilepath = $outputFilepath");

    val jDBCConnection = mappingExecution.jdbcConnection

    //val distribution = dataset.getDistribution();
    val randomUUID = UUID.randomUUID.toString
    val databaseName =  s"executions/${md.dctIdentifier}/${randomUUID}"
    logger.info(s"databaseName = $databaseName")

    val properties: MorphRDBProperties = new MorphRDBProperties
    properties.setNoOfDatabase(1)
    properties.setDatabaseUser(jDBCConnection.dbUserName)
    properties.setDatabasePassword(jDBCConnection.dbPassword)
    properties.setDatabaseName(jDBCConnection.dbName)
    properties.setDatabaseURL(jDBCConnection.jdbc_url)
    properties.setDatabaseDriver(jDBCConnection.databaseDriver)
    properties.setDatabaseType(jDBCConnection.databaseType)
    properties.setMappingDocumentFilePath(mappingDocumentDownloadURL)
    properties.setOutputFilePath(outputFilepath)


    val runnerFactory: MorphRDBRunnerFactory = new MorphRDBRunnerFactory
    val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
    runner.run
  }

  def executeMapping(mappingExecution:MappingExecution) = {
    val md = mappingExecution.mappingDocument;
    val mappingLanguage =
      if (md.mappingLanguage == null) { MappingPediaConstant.MAPPING_LANGUAGE_R2RML }
      else { md.mappingLanguage }

    if (MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {
      //this.morphRDBQueue.enqueue(mappingExecution)
      MappingExecutionController.executeR2RMLMapping(mappingExecution);
    } else if (MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
      MappingExecutionController.executeRMLMapping(mappingExecution);
    } else if (MappingPediaConstant.MAPPING_LANGUAGE_xR2RML.equalsIgnoreCase(mappingLanguage)) {
      throw new Exception(mappingLanguage + " Language is not supported yet");
    } else {
      throw new Exception(mappingLanguage + " Language is not supported yet");
    }
    logger.info("mapping execution done!")
  }

  def executeR2RMLMapping(mappingExecution:MappingExecution) = {
    if(mappingExecution.jdbcConnection != null) {
      this.executeR2RMLMappingWithRDB(mappingExecution)
    } else if(mappingExecution.unannotatedDistributions != null) {
      this.executeR2RMLMappingWithCSV(mappingExecution);
    }
  }

  def executeR2RMLMappingWithCSV(mappingExecution:MappingExecution) = {
    logger.info("Executing R2RML mapping (CSV) ...")
    val md = mappingExecution.mappingDocument;
    val unannotatedDistributions = mappingExecution.unannotatedDistributions
    val queryFileName = mappingExecution.queryFileName
    val outputFilepath = if(mappingExecution.outputDirectory == null) { mappingExecution.getOutputFileWithExtension; }
    else { s"${mappingExecution.outputDirectory}/${mappingExecution.getOutputFileWithExtension}"}
    logger.info(s"outputFilepath = $outputFilepath");

    val mappingDocumentDownloadURL = md.getDownloadURL();
    logger.info(s"mappingDocumentDownloadURL = $mappingDocumentDownloadURL");

    //val distributions = unannotatedDataset.dcatDistributions
    val downloadURLs = unannotatedDistributions.map(distribution => distribution.dcatDownloadURL);

    val datasetDistributionDownloadURL = downloadURLs.mkString(",")
    logger.info(s"datasetDistributionDownloadURL = $datasetDistributionDownloadURL");

    val csvSeparator = unannotatedDistributions.iterator.next().csvFieldSeparator;

    val randomUUID = UUID.randomUUID.toString
    val databaseName =  s"executions/${md.dctIdentifier}/${randomUUID}"
    logger.info(s"databaseName = $databaseName")

    val properties: MorphCSVProperties = new MorphCSVProperties
    properties.setDatabaseName(databaseName)
    properties.setMappingDocumentFilePath(mappingDocumentDownloadURL)
    properties.setOutputFilePath(outputFilepath);
    properties.setCSVFile(datasetDistributionDownloadURL);
    properties.setQueryFilePath(queryFileName);
    if (csvSeparator != null) {
      properties.fieldSeparator = Some(csvSeparator);
    }

    val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
    val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
    runner.run
  }

  def executeRMLMapping(mappingExecution: MappingExecution) = {
    logger.info("Executing RML mapping ...")
    val rmlConnector = new RMLMapperConnector();
    rmlConnector.executeWithMain(mappingExecution);
  }

  def storeManifestOnVirtuoso(manifestFile:File) = {
    if(manifestFile != null) {
      logger.info("storing the manifest triples of a mapping execution result on virtuoso ...")
      logger.debug("manifestFile = " + manifestFile);
      MappingPediaEngine.virtuosoClient.storeFromFile(manifestFile)
      logger.info("manifest triples stored on virtuoso.")
      "OK";
    } else {
      "No manifest file specified/generated!";
    }
  }

  def getMappingExecutionResultURL(mdSHA:String, datasetDistributionSHA:String) = {

  }

  def generateManifestFile(mappingExecutionResult:AnnotatedDistribution
                           //, datasetDistribution: Distribution
                           , unannotatedDistributions: List[UnannotatedDistribution]
                           , mappingDocument:MappingDocument) = {




    logger.info("Generating manifest file for Mapping Execution Result ...")
    try {
      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-mappingexecutionresult-template.ttl"
      );

      //val datasetDistributionDownloadURL:String = "";

      val downloadURL = if(mappingExecutionResult.dcatDownloadURL == null) { "" }
      else { mappingExecutionResult.dcatDownloadURL }
      logger.info(s"downloadURL = ${downloadURL}")

      val mappingDocumentHash = if(mappingDocument.hash == null) { "" } else { mappingDocument.hash }
      logger.info(s"mappingDocumentHash = ${mappingDocumentHash}")

      //val datasetDistributionHash = unannotatedDataset.dcatDistributions.hashCode().toString
      val datasetDistributionHash = MappingPediaUtility.calculateHash(unannotatedDistributions);
      logger.info(s"datasetDistributionHash = ${datasetDistributionHash}")

      val datasetId = mappingExecutionResult.dataset.dctIdentifier;

      val mapValues:Map[String,String] = Map(
        "$mappingExecutionResultID" -> mappingExecutionResult.dctIdentifier
        , "$mappingExecutionResultTitle" -> mappingExecutionResult.dctTitle
        , "$mappingExecutionResultDescription" -> mappingExecutionResult.dctDescription
        , "$datasetID" -> datasetId
        , "$mappingDocumentID" -> mappingDocument.dctIdentifier
        , "$downloadURL" -> downloadURL
        , "$mappingDocumentHash" -> mappingDocumentHash
        , "$datasetDistributionHash" -> datasetDistributionHash
        , "$issued" -> mappingExecutionResult.dctIssued
        , "$modified" -> mappingExecutionResult.dctModified

      );

      val manifestString = MappingPediaEngine.generateManifestString(mapValues, templateFiles);
      val filename = s"metadata-mappingexecutionresult-${mappingExecutionResult.dctIdentifier}.ttl";
      val manifestFile = MappingPediaEngine.generateManifestFile(manifestString, filename, datasetId);
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

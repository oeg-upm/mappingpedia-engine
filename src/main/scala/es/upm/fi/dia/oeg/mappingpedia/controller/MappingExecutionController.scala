package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.{HttpURLConnection, URL}
import java.util.{Date, UUID}

import com.mashape.unirest.http.{HttpResponse, JsonNode, Unirest}
import es.upm.fi.dia.oeg.mappingpedia.model.result.{ExecuteMappingResult, ExecuteMappingResultSummary, GeneralResult, ListResult}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.model._
import es.upm.fi.dia.oeg.mappingpedia.utility._
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory, MorphRDBProperties, MorphRDBRunnerFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.query.QueryFactory
import org.apache.jena.query.QueryExecutionFactory
import java.io.InputStream

import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}


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
                      //, unannotatedDistributions: List[UnannotatedDistribution]
                    //, unannotatedDistribution: UnannotatedDistribution
                    , unannotatedDataset:Dataset
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

    val organization = unannotatedDataset.dctPublisher
    //val unannotatedDistribution = dataset.getDistribution().asInstanceOf[UnannotatedDistribution];

    //val datasetDistributionDownloadURL = unannotatedDistribution.dcatDownloadURL;
    //logger.info(s"datasetDistributionDownloadURL = ${datasetDistributionDownloadURL}");

    /*
    val datasetDistributionSHA = unannotatedDistribution.sha;
    if (unannotatedDistribution.sha == null && unannotatedDistribution.dcatDownloadURL != null ) {
      val hashValue = MappingPediaUtility.calculateHash(unannotatedDistribution.dcatDownloadURL
        , unannotatedDistribution.encoding);
      unannotatedDistribution.sha = hashValue
    }
    */

    val unannotatedDistributions = unannotatedDataset.dcatDistributions.asInstanceOf[List[UnannotatedDistribution]]
    val distributionsHash = unannotatedDistributions.hashCode().toString;

    val mdDownloadURL = md.getDownloadURL();
    if (md.sha == null && mdDownloadURL != null ) {
      val hashValue = MappingPediaUtility.calculateHash(mdDownloadURL, "UTF-8");
      md.sha = hashValue
    }


    val cacheExecutionURL = this.findMappingExecutionURLBySHA(md.sha, distributionsHash);
    logger.info(s"cacheExecutionURL = ${cacheExecutionURL}");

    if(cacheExecutionURL == null || cacheExecutionURL.results.isEmpty || !useCache) {
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

      val datasetId = unannotatedDataset.dctIdentifier

      val mappingExecutionId = UUID.randomUUID.toString;
      val annotatedDistribution = new AnnotatedDistribution(unannotatedDataset, mappingExecutionId)
      annotatedDistribution.dcatMediaType = "text/txt" //TODO FIXME
      annotatedDistribution.dctDescription = "Annotated Dataset using the annotation: " + mdDownloadURL;



      /*
      val mappingExecutionDirectory = if(organization != null && dataset != null) {
        organization.dctIdentifier + File.separator + dataset.dctIdentifier
      } else { UUID.randomUUID.toString }
      */

      //val mappingExecutionDirectory = s"executions/$organizationId/$datasetId/$mappingExecutionId";
      val mappingExecutionDirectory = s"executions/$organizationId/$datasetId/${md.dctIdentifier}";
      //val mappingExecutionDirectory = s"executions/$organizationId/$datasetId/${md.dctIdentifier}/$mappingExecutionId";

      val outputFileName = if (pOutputFilename == null) {
        UUID.randomUUID.toString + ".txt"
      } else {
        pOutputFilename;
      }
      val githubOutputFilepath:String = s"$mappingExecutionDirectory/$outputFileName"
      val localOutputFilepath:String = s"$mappingExecutionDirectory/$mappingExecutionId/$outputFileName"
      val localOutputFile: File = new File(localOutputFilepath)
      annotatedDistribution.distributionFile = localOutputFile;

      //EXECUTING MAPPING
      try {
        if (MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {
          if(jdbcConnection != null) {
            MappingExecutionController.executeR2RMLMappingWithRDB(md, localOutputFilepath, queryFileName, jdbcConnection);
          } else {
            MappingExecutionController.executeR2RMLMappingWithCSV(md, unannotatedDistributions, localOutputFilepath, queryFileName);
          }
        } else if (MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
          val unannotatedDistribution = unannotatedDistributions.iterator.next();
          MappingExecutionController.executeRMLMapping(md, unannotatedDataset, localOutputFilepath);
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


      //GENERATING MANIFEST FILE
      val manifestFile = this.generateManifestFile(annotatedDistribution, unannotatedDataset, md)

      //STORING MANIFEST ON GITHUB
      val addManifestFileGitHubResponse:HttpResponse[JsonNode] =
        if(MappingPediaEngine.mappingpediaProperties.githubEnabled && pStoreToGithub) {
          try {
            this.storeManifestFileOnGitHub(manifestFile, unannotatedDataset, md);
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


      val mappedClassByMappingId:String = try {
        this.mappingDocumentController.findMappedClassesByMappingDocumentId(
          md.dctIdentifier).results.iterator.next().toString
      } catch {
        case e:Exception => null
      }

      /*
      val mappedClassByDownloadURL:String = try {
        if(mappedClassByMappingId == null) {
          val documentUrl = new URL("http://example.org/example.ttl")
          val inputStream = documentUrl.openStream
          val rdfParser = Rio.createParser(RDFFormat.TURTLE)
          val model = new LinkedHashModel()
          rdfParser.setRDFHandler(new StatementCollector(model))

          try {
            rdfParser.parse(inputStream, documentUrl.toString)
            logger.info(s"model = ${model}");
          } catch {
            case e: Exception => {
              e.printStackTrace()
            }
          } finally { inputStream.close }

          mappedClassByMappingId
        } else {
          mappedClassByMappingId
        }
      }  catch {
        case e:Exception => {
          mappedClassByMappingId
        }
      }
      */






/*      val mappedClassByDownloadURL:String = if(mappedClassByMappingId == null) {
        val model = ModelFactory.createDefaultModel();
        model.read(md.getDownloadURL(), "TURTLE");
        logger.info(s"model = ${model}");

        val mapValues: Map[String, String] = Map(
          "$graphURL" -> MappingPediaEngine.mappingpediaProperties.graphName
        );

        val queryString: String = MappingPediaEngine.generateStringFromTemplateFile(
          mapValues, "templates/findMappedClasses.rq")
        logger.info(s"queryString = ${queryString}");

        val query = QueryFactory.create(queryString)

        val qexec = QueryExecutionFactory.create(query, model)
        try {
          val results = qexec.execSelect

          if ( { results.hasNext }) {
            val soln = results.nextSolution
            val solnMappedClass = soln.get("mappedClass")
            logger.info(s"solnMappedClass = ${solnMappedClass}");

            // Get a result variable - must be a resource
            val mappedClassLiteral = soln.getLiteral("mappedClass") // Get a result variable - must be a literal
            logger.info(s"mappedClassLiteral = ${mappedClassLiteral}");

            // Get a result variable by name.
            val mappedClassResource = soln.getResource("mappedClass")
            logger.info(s"mappedClassResource = ${mappedClassResource}");

            solnMappedClass.toString
          } else {
            null
          }

        } finally if (qexec != null) qexec.close()

      } else {
        mappedClassByMappingId
      }
      logger.info(s"mappedClassByDownloadURL = $mappedClassByDownloadURL")*/

      val mappedClass = mappedClassByMappingId;
      logger.info(s"mappedClass = $mappedClass")

      //STORING MAPPING EXECUTION RESULT AS A RESOURCE ON CKAN
      val ckanAddResourceResponse = try {
        if(MappingPediaEngine.mappingpediaProperties.ckanEnable && pStoreToCKAN) {
          logger.info("storing mapping execution result on CKAN ...")


          //val addNewResourceResponse = CKANUtility.addNewResource(resourceIdentifier, resourceTitle
          //            , resourceMediaType, resourceFileRef, resourceDownloadURL)
          //val addNewResourceResponse = CKANUtility.addNewResource(distribution);

          val unannotatedDistributionsDownloadURLs = unannotatedDistributions.map(distribution => distribution.dcatDownloadURL);

          val mapTextBody:Map[String, String] = Map(
            MappingPediaConstant.CKAN_RESOURCE_ORIGINAL_DATASET_DISTRIBUTION_DOWNLOAD_URL -> unannotatedDistributionsDownloadURLs.mkString(",")
            , MappingPediaConstant.CKAN_RESOURCE_MAPPING_DOCUMENT_DOWNLOAD_URL -> md.getDownloadURL()
            , MappingPediaConstant.CKAN_RESOURCE_PROV_TRIPLES -> annotatedDistribution.manifestDownloadURL
            , MappingPediaConstant.CKAN_RESOURCE_CLASS -> mappedClass
          )
          ckanClient.createResource(annotatedDistribution, Some(mapTextBody));

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
        //, unannotatedDistribution
        , md
        , queryFileName
        , annotatedDistribution
      )
    } else {
      val mappingExecutionResultURL = cacheExecutionURL.results.iterator.next().toString;
      val annotatedDistribution = new AnnotatedDistribution(unannotatedDataset);
      annotatedDistribution.dcatDownloadURL = mappingExecutionResultURL;

      new ExecuteMappingResult(
        HttpURLConnection.HTTP_OK, "OK"
        //, unannotatedDistribution
        , md
        , queryFileName
        , annotatedDistribution
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
    val executionResults:Iterable[ExecuteMappingResult] = mappingDocuments.flatMap(
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
        val unannotatedDistribution = new UnannotatedDistribution(dataset);
        dataset.addDistribution(unannotatedDistribution);
        unannotatedDistribution.dcatDownloadURL = md.dataset.getDistribution().dcatDownloadURL;
        unannotatedDistribution.sha = md.dataset.getDistribution().sha
        if (unannotatedDistribution.sha == null && unannotatedDistribution.dcatDownloadURL != null ) {
          val hashValue = MappingPediaUtility.calculateHash(unannotatedDistribution.dcatDownloadURL, unannotatedDistribution.encoding);
          unannotatedDistribution.sha = hashValue
        }


        logger.info(s"mapping document SHA = ${md.sha}");
        logger.info(s"dataset distribution SHA = ${unannotatedDistribution.sha}");

        if(executedMappingDocuments.contains((md.sha,unannotatedDistribution.sha))) {
          None
        } else {
          if(i < maxMappingDocuments) {
            val mappingExecutionURLs = if(useCache) { this.findMappingExecutionURLBySHA(md.sha,unannotatedDistribution.sha); }
            else { null }

            if(useCache && mappingExecutionURLs != null && mappingExecutionURLs.results.size > 0) {
              val cachedMappingExecutionResultURL:String = mappingExecutionURLs.results.iterator.next().toString;
              logger.info(s"cachedMappingExecutionResultURL = " + cachedMappingExecutionResultURL)

              i +=1
              //Some(new ExecuteMappingResultSummary(md, unannotatedDistribution, cachedMappingExecutionResultURL, cachedMappingExecutionResultURL))
              val mappingExecutionResult = new AnnotatedDistribution(dataset);
              mappingExecutionResult.dcatAccessURL = cachedMappingExecutionResultURL;
              mappingExecutionResult.dcatDownloadURL = cachedMappingExecutionResultURL;

              Some(new ExecuteMappingResult(HttpURLConnection.HTTP_OK, "OK"
                //, unannotatedDistribution
                , md, null, mappingExecutionResult))
            } else {
              val mappingExecution = new MappingExecution(md, dataset);
              mappingExecution.setStoreToCKAN("false")
              mappingExecution.queryFilePath = null;
              mappingExecution.outputFileName = outputFilename;

              //THERE IS NO NEED TO STORE THE EXECUTION RESULT IN THIS PARTICULAR CASE
              val executionResult = this.executeMapping(md
                , dataset
                , mappingExecution.queryFilePath, outputFilename
                , false, true, false
                , null
                , useCache
              );
              //val executionResult = MappingExecutionController.executeMapping2(mappingExecution);

              executedMappingDocuments = (md.sha,unannotatedDistribution.sha) :: executedMappingDocuments;

              val executionResultAccessURL = executionResult.getMapping_execution_result_access_url()
              val executionResultDownloadURL = executionResult.getMapping_execution_result_download_url
              //executionResultURL;

              i +=1
              //Some(new ExecuteMappingResultSummary(md, unannotatedDistribution, executionResultAccessURL, executionResultDownloadURL))
              Some(executionResult);
            }
          } else {
            None
          }
        }



      })
    new ListResult(executionResults.size, executionResults);

  }

  def generateManifestFile(mappingExecutionResult:Distribution
                           //, datasetDistribution: Distribution
                           //, unannotatedDistributions: List[UnannotatedDistribution]
                          , unannotatedDataset: Dataset
                           , mappingDocument:MappingDocument) = {


    logger.info("Generating manifest file for Mapping Execution Result ...")
    try {
      val templateFiles = List(
        "templates/metadata-namespaces-template.ttl"
        , "templates/metadata-mappingexecutionresult-template.ttl"
      );

      val datasetDistributionDownloadURL:String = "";

      val downloadURL = if(mappingExecutionResult.dcatDownloadURL == null) { "" }
      else { mappingExecutionResult.dcatDownloadURL }
      logger.info(s"downloadURL = ${downloadURL}")

      val mappingDocumentSHA = if(mappingDocument.sha == null) { "" } else { mappingDocument.sha }
      logger.info(s"mappingDocumentSHA = ${mappingDocumentSHA}")

      val datasetDistributionSHA = ""

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
      val manifestFile = MappingPediaEngine.generateManifestFile(mapValues, templateFiles, filename, unannotatedDataset.dctIdentifier);
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


  def executeR2RMLMappingWithRDB(md:MappingDocument
                                 , outputFilepath:String, queryFileName:String
                                 /*
                                 , dbUserName:String, dbPassword:String
                                 , dbName:String, jdbc_url:String
                                 , databaseDriver:String, databaseType:String
                                 */
                                 , jdbcConnection: JDBCConnection

                                ) = {
    logger.info("Executing R2RML mapping ...")
    //val distribution = dataset.getDistribution();
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

  def executeR2RMLMappingWithCSV(md:MappingDocument
                                 , unannotatedDistributions: List[UnannotatedDistribution]
                                //, unannotatedDataset:Dataset
                                 , outputFilepath:String, queryFileName:String) = {
    logger.info("Executing R2RML mapping ...")
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

  def executeRMLMapping(md:MappingDocument
                        , unannotatedDataset: Dataset
                        , outputFilepath:String) = {
    logger.info("Executing RML mapping ...")

    val unannotatedDistribution = unannotatedDataset.getDistribution();
    val datasetDistributionDownloadURL = unannotatedDistribution.dcatDownloadURL;
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

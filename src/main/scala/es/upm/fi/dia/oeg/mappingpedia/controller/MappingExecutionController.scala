package es.upm.fi.dia.oeg.mappingpedia.controller

import java.io.File
import java.net.HttpURLConnection
import java.util.UUID

import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaConstant, MappingPediaEngine}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.logger
import es.upm.fi.dia.oeg.mappingpedia.connector.RMLMapperConnector
import es.upm.fi.dia.oeg.mappingpedia.model.MappingPediaExecutionResult
import es.upm.fi.dia.oeg.mappingpedia.utility.GitHubUtility
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.{MorphCSVProperties, MorphCSVRunnerFactory}
import org.apache.commons.lang.text.StrSubstitutor

object MappingExecutionController {
  def executeMapping1(mappingpediaUsername:String, mappingDirectory: String
                     , mappingFilename: String, datasetFile: String
                     , queryFile:String, pOutputFilename: String) : MappingPediaExecutionResult = {
    logger.debug("mappingpediaUsername = " + mappingpediaUsername)
    logger.debug("mappingDirectory = " + mappingDirectory)
    logger.debug("mappingFilename = " + mappingFilename)
    val properties: MorphCSVProperties = new MorphCSVProperties
    properties.setDatabaseName(mappingpediaUsername + "/" + mappingDirectory)
    val templateString: String = "${mappingpediaUsername}/${mappingDirectory}/${mappingFilename}"
    val valuesMap: java.util.Map[String, String] = new java.util.HashMap[String, String]
    valuesMap.put("mappingpediaUsername", mappingpediaUsername)
    valuesMap.put("mappingDirectory", mappingDirectory)
    valuesMap.put("mappingFilename", mappingFilename)
    val sub: StrSubstitutor = new StrSubstitutor(valuesMap)
    val templateResultString: String = sub.replace(templateString)
    val githubRepo = MappingPediaEngine.mappingpediaProperties.githubRepo
    val mappingBlobURL: String = githubRepo + "/blob/master/" + templateResultString
    //val mappingBlobURL: String = "https://github.com/oeg-upm/mappingpedia-contents/blob/master/" + templateResultString
    logger.debug("mappingBlobURL = " + mappingBlobURL)
    properties.setMappingDocumentFilePath(mappingBlobURL)
    val outputFileName = if (pOutputFilename == null) {
      //"output.nt";
      MappingPediaConstant.DEFAULT_OUTPUT_FILENAME;
    } else {
      pOutputFilename;
    }
    val outputFilepath = "executions/" + templateResultString + "/" + outputFileName

    properties.setOutputFilePath(outputFilepath);



    properties.setCSVFile(datasetFile);
    logger.debug("datasetFile = " + datasetFile)

    properties.setQueryFilePath(queryFile);
    try {
      val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
      val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
      runner.run
      logger.info("mapping execution success!")
      val outputFile: File = new File(outputFilepath)
      val response = GitHubUtility.putEncodedFile(MappingPediaEngine.mappingpediaProperties.githubUser
        , MappingPediaEngine.mappingpediaProperties.githubAccessToken
        , mappingpediaUsername, mappingDirectory, outputFileName
        , "add mapping execution result by mappingpedia engine", outputFile);

      val responseStatus: Int = response.getStatus
      logger.info("responseStatus = " + responseStatus)
      val responseStatusText: String = response.getStatusText
      logger.info("responseStatusText = " + responseStatusText)
      if (HttpURLConnection.HTTP_CREATED== responseStatus || HttpURLConnection.HTTP_OK == responseStatus) {
        val outputGitHubURL: String = response.getBody.getObject.getJSONObject("content").getString("url");
        val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null
          ,null , outputGitHubURL, responseStatusText, responseStatus, null)
        return executionResult
      }
      else {
        val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null
          , null, null, responseStatusText, responseStatus, null)
        return executionResult
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace
        val errorMessage: String = "Error occured: " + e.getMessage
        logger.error("mapping execution failed: " + errorMessage)
        val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null
          , null, null, errorMessage, HttpURLConnection.HTTP_INTERNAL_ERROR, null)
        return executionResult
      }
    }
  }

  @throws(classOf[Exception])
  def executeMapping2(mappingURL: String, pMappingLanguage:String
                      , datasetDistributionURL: String, fieldSeparator:String
                      , queryFile:String, pOutputFilename: String
                      , organizationId:String, datasetId:String, storeToCKAN:String
                     ) : MappingPediaExecutionResult = {
    val mappingLanguage = if (pMappingLanguage == null) {
      MappingPediaConstant.MAPPING_LANGUAGE_R2RML
    } else {
      pMappingLanguage
    }

    //val mappingpediaUsername = "executions"
    val mappingDirectory = UUID.randomUUID.toString
    val outputFileName = if (pOutputFilename == null) {
      //"output.nt";
      //MappingPediaConstant.DEFAULT_OUTPUT_FILENAME;
      UUID.randomUUID.toString
    } else {
      pOutputFilename;
    }
    val outputFilepath = "executions/" + mappingDirectory + "/" + outputFileName

    try {
      if (MappingPediaConstant.MAPPING_LANGUAGE_R2RML.equalsIgnoreCase(mappingLanguage)) {

        val properties: MorphCSVProperties = new MorphCSVProperties
        properties.setDatabaseName("executions/" + mappingDirectory)
        properties.setMappingDocumentFilePath(mappingURL)
        properties.setOutputFilePath(outputFilepath);
        properties.setCSVFile(datasetDistributionURL);
        properties.setQueryFilePath(queryFile);
        if (fieldSeparator != null) {
          properties.fieldSeparator = Some(fieldSeparator);
        }

        val runnerFactory: MorphCSVRunnerFactory = new MorphCSVRunnerFactory
        val runner: MorphBaseRunner = runnerFactory.createRunner(properties)
        runner.run
      } else if (MappingPediaConstant.MAPPING_LANGUAGE_RML.equalsIgnoreCase(mappingLanguage)) {
        val rmlConnector = new RMLMapperConnector();
        //rmlConnector.execute(mappingURL, outputFilepath);
        rmlConnector.executeWithMain(datasetDistributionURL, mappingURL, outputFilepath);

      } else if (MappingPediaConstant.MAPPING_LANGUAGE_xR2RML.equalsIgnoreCase(mappingLanguage)) {
        throw new Exception(mappingLanguage + " Language is not supported yet");
      } else {
        throw new Exception(mappingLanguage + " Language is not supported yet");
      }

      logger.info("mapping execution success!")
      val outputFile: File = new File(outputFilepath)
      val response = GitHubUtility.putEncodedFile(MappingPediaEngine.mappingpediaProperties.githubUser
        , MappingPediaEngine.mappingpediaProperties.githubAccessToken
        , "executions", mappingDirectory, outputFileName
        , "add mapping execution result by mappingpedia engine", outputFile);

      val responseStatus: Int = response.getStatus
      logger.info("responseStatus = " + responseStatus)
      val responseStatusText: String = response.getStatusText
      logger.info("responseStatusText = " + responseStatusText)
      if (HttpURLConnection.HTTP_CREATED == responseStatus || HttpURLConnection.HTTP_OK == responseStatus) {
        val outputGitHubURL: String = response.getBody.getObject.getJSONObject("content").getString("url");
        val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, datasetDistributionURL, mappingURL
          , queryFile, outputGitHubURL, responseStatusText, responseStatus, null)


        return executionResult
      }
      else {
        val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, datasetDistributionURL, mappingURL
          , queryFile, null, responseStatusText, responseStatus, null)
        return executionResult
      }

    }
    catch {
      case e: Exception => {
        e.printStackTrace
        val errorMessage: String = "Error occured: " + e.getMessage
        logger.error("mapping execution failed: " + errorMessage)
        val executionResult: MappingPediaExecutionResult = new MappingPediaExecutionResult(null, null, null
          , null, null, errorMessage, HttpURLConnection.HTTP_INTERNAL_ERROR, null)
        return executionResult
      }
    }
  }
}

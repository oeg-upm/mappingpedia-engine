package es.upm.fi.dia.oeg.mappingpedia.controller

import java.net.HttpURLConnection

import com.fasterxml.jackson.databind.ObjectMapper
import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.model.MappingExecution
import es.upm.fi.dia.oeg.mappingpedia.model.result.ExecuteMappingResult
import es.upm.fi.dia.oeg.mappingpedia.utility.JenaClient
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.http.HttpStatus

import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class MappingExecutionControllerHelper(
                                        mappingExecutionController: MappingExecutionController
                                      ) extends Runnable {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);
  var executionQueue = new mutable.Queue[MappingExecution];
  var isProcessing = false;

  override def run(): Unit = {
    while(true) {
      try {
        //logger.info(s"${this} executionQueue = ${executionQueue.size}")
        //logger.info(s"${this} isProcessing = ${isProcessing}")

        if(executionQueue.size > 0 && !isProcessing) {
          isProcessing = true;
          val mappingExecution = executionQueue.dequeue();

          val f = mappingExecutionController.executeMappingWithFuture(mappingExecution);
          val mapper = new ObjectMapper();
          val callbackURL = mappingExecution.callbackURL
          val executeMappingResult = if(callbackURL == null) {
            logger.info("Await.result");
            val result = Await.result(f, 60 second)
            result;
          } else {
            f.onComplete {
              case Success(forkExecuteMappingResult:ExecuteMappingResult) => {
                logger.info("f.onComplete Success");

                val forkExecuteMappingResultAsString = mapper.writeValueAsString(forkExecuteMappingResult)
                logger.info(s"forkExecuteMappingResultAsString = ${forkExecuteMappingResultAsString}");

                //val mappingExecutionResultDownloadURL = forkExecuteMappingResult.getMapping_execution_result_download_url;
                //logger.info(s"mappingExecutionResultDownloadURL = ${mappingExecutionResultDownloadURL}");

                /*
                val field = if(callbackField == null || "".equals(callbackField)) {
                  "notification"
                } else { callbackField }
                */

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

                isProcessing = false;
              }
              case Failure(e) => {

                logger.info("f.onComplete Success Failure");
                e.printStackTrace
                isProcessing = false;
              }
            }

            logger.info("In Progress");
            new ExecuteMappingResult(
              HttpURLConnection.HTTP_ACCEPTED, HttpStatus.ACCEPTED.getReasonPhrase
              , mappingExecution
              , null
            )
          }
        }
        Thread.sleep(1000) // wait for 1000 millisecond
      } catch {
        case e:Exception => {
          e.printStackTrace()
        }
      }
    }
  }

}

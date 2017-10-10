package es.upm.fi.dia.oeg.mappingpedia.model

import com.mashape.unirest.http.{HttpResponse, JsonNode, Unirest}

class MappingPediaExecutionResult(val manifestURL:String, val datasetURL:String, val mappingURL:String
                                  , val queryURL:String, val mappingExecutionResultURL:String
                                  , val status:String, val errorCode:Integer
                                  //, val virtuosoStatus:String, val githubStatus:String
                                  , val ckanResponse:String
                                 ) {
  val mappingExecutionResultDownloadURL = try {
    val response = Unirest.get(mappingExecutionResultURL).asJson();
    response.getBody.getObject.getString("download_url");
  } catch {
    case e:Exception => mappingExecutionResultURL
  }

  def getManifestURL() = manifestURL;
  def getDatasetURL() = datasetURL;
  def getMappingURL() = mappingURL;
  def getQueryURL() = queryURL;
  def getMappingExecutionResultURL() = mappingExecutionResultURL;
  def getStatus() = status;
  def getErrorCode() = errorCode;
  def getMappingExecutionResultDownloadURL = mappingExecutionResultDownloadURL;

  //def getVirtuosoStatus = virtuosoStatus;
  //def getGithubStatus = githubStatus;
  def getCKANResponse = ckanResponse;
}
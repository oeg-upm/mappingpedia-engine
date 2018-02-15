package es.upm.fi.dia.oeg.mappingpedia.model.result

import com.mashape.unirest.http.Unirest

class GeneralResult(
                     val status:String, val errorCode:Integer
                     //val manifestURL:String, val datasetURL:String, val mappingURL:String
                     //, val queryURL:String, val mappingExecutionResultURL:String

                     //, val ckanResponse:String
                   )
{
  def getStatus() = status;
  def getStatus_code = errorCode;

  /*
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

  def getMappingExecutionResultDownloadURL = mappingExecutionResultDownloadURL;

  def getCKANResponse = ckanResponse;
  */
}
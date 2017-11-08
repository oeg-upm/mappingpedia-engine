package es.upm.fi.dia.oeg.mappingpedia.model.result

class ExecuteMappingResult (
                             val errorCode:Integer, val status:String
                             , val datasetURL:String, val mappingURL:String
                             , val queryURL:String
                             , val mappingExecutionResultURL:String, val mappingExecutionResultDownloadURL:String
                             , val ckanResponseStatus:Integer
                           , val mappingExecutionResultId:String
                           , val manifestURL:String
                           ){

  def getDatasetURL() = datasetURL;
  def getDataset_access_url() = datasetURL;

  def getMappingURL() = mappingURL;
  def getMapping_document_download_url() = mappingURL;

  def getQueryURL() = queryURL;
  def getQuery_file_download_url() = queryURL;


  def getMappingExecutionResultURL() = mappingExecutionResultURL;
  def getMapping_execution_result_access_url() = mappingExecutionResultURL;

  def getStatus() = status;
  def getErrorCode() = errorCode;
  def getStatus_code() = errorCode;

  def getMappingExecutionResultDownloadURL = mappingExecutionResultDownloadURL;
  def getMapping_execution_result_download_url = mappingExecutionResultDownloadURL;

  def getCKANResponseStatus = ckanResponseStatus;
  def getCKAN_response_status = ckanResponseStatus;

  def getMappingExecutionResultId = this.mappingExecutionResultId
  def getMapping_execution_result_id = this.mappingExecutionResultId

  def getManifestURL = this.manifestURL
  def getManifest_url = this.manifestURL

}

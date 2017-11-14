package es.upm.fi.dia.oeg.mappingpedia.model.result

class ExecuteMappingResult (
                             val errorCode:Integer, val status:String
                             , val distributionDownloadURL:String
                             , val mappingDocumentDownloadURL:String
                             , val queryURL:String
                             , val mappingExecutionResultURL:String, val mappingExecutionResultDownloadURL:String
                             , val ckanResponseStatus:Integer
                           , val mappingExecutionResultId:String
                           , val manifestAccessURL:String
                           ){

  def getDatasetURL() = distributionDownloadURL;
  def getDistribution_download_url() = distributionDownloadURL;

  def getMappingURL() = mappingDocumentDownloadURL;
  def getMapping_document_download_url() = mappingDocumentDownloadURL;

  def getQueryURL() = queryURL;
  def getQuery_file_download_url() = queryURL;

  def getMappingExecutionResultURL() = mappingExecutionResultURL;
  def getMapping_execution_result_access_url() = mappingExecutionResultURL;
  def getMapping_execution_result_download_url = mappingExecutionResultDownloadURL;

  def getStatus() = status;
  def getErrorCode() = errorCode;
  def getStatus_code() = errorCode;

  def getCKAN_response_status = ckanResponseStatus;

  def getMapping_execution_result_id = this.mappingExecutionResultId

  def getManifestURL = this.manifestAccessURL
  def getManifest_access_url = this.manifestAccessURL

}

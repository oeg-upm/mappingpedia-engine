package es.upm.fi.dia.oeg.mappingpedia.model.result

import es.upm.fi.dia.oeg.mappingpedia.model.{Distribution, MappingDocument}

class ExecuteMappingResult (
                             val errorCode:Integer, val status:String
                             , val distributionDownloadURL:String
                             , val distribution:Distribution
                             , val mappingDocumentDownloadURL:String
                             , val mappingDocument:MappingDocument
                             , val queryURL:String
                             , val mappingExecutionResultURL:String
                             , val mappingExecutionResultDownloadURL:String
                             , val ckanResponseStatus:Integer
                             , val mappingExecutionResultId:String
                             , val manifestAccessURL:String
                             , val manifestDownloadURL:String
                           ){

  def this(errorCode:Integer, status:String) {
    this(errorCode, status
      , null, null, null, null, null
      , null, null, null, null, null
    , null )
  }

  def getDatasetURL() = distributionDownloadURL;
  def getDistribution_download_url() = distributionDownloadURL;
  def getDistribution_sha() = distribution.getSHA;

  def getMappingURL() = mappingDocumentDownloadURL;
  def getMapping_document_download_url() = mappingDocumentDownloadURL;
  def getMapping_document_sha() = mappingDocument.getSHA;

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
  def getManifest_download_url = this.manifestDownloadURL

}

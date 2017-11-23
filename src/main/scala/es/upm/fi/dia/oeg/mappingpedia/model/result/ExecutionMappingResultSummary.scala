package es.upm.fi.dia.oeg.mappingpedia.model.result

import es.upm.fi.dia.oeg.mappingpedia.model.{Distribution, MappingDocument}

//TODO Refactor this
class ExecutionMappingResultSummary(
                  //val errorCode:Integer, val status:String
                  val mappingDocument:MappingDocument
                  , val distribution:Distribution
                  , val mappingExecutionResultAccessURL:String, val mappingExecutionResultDownloadURL:String
                )
{
  def getDistribution_download_url = distribution.dcatDownloadURL
  def getMapping_document_download_url = mappingDocument.getDownloadURL()
  def getMapping_execution_result_access_url = this.mappingExecutionResultAccessURL
  def getMapping_execution_result_download_url = this.mappingExecutionResultDownloadURL
  def getMapping_document_sha = mappingDocument.sha;
  def getDistribution_sha = distribution.sha;

}


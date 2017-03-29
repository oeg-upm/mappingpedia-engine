package es.upm.fi.dia.oeg.mappingpedia.r2rml

class MappingPediaExecutionResult(val manifestURL:String, val datasetURL:String, val mappingURL:String
                                 , val queryURL:String, val mappingExecutionResultURL:String
                                  , val status:String, val errorCode:Integer) {
  def getManifestURL() = manifestURL;
  def getDatasetURL() = datasetURL;
  def getMappingURL() = mappingURL;
  def getQueryURL() = queryURL;
  def getMappingExecutionResultURL() = mappingExecutionResultURL;
  def getStatus() = status;
  def getErrorCode() = errorCode;
}
package es.upm.fi.dia.oeg.mappingpedia.r2rml

class MappingPediaExecutionResult(val manifestURL:String, val mappingURL:String, val status:String, val errorCode:Integer) {
  def getManifestURL() = manifestURL;
  def getMappingURL() = mappingURL;
  def getStatus() = status;
  def getErrorCode() = errorCode;
}
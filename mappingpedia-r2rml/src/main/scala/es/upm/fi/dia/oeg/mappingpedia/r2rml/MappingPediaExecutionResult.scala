package es.upm.fi.dia.oeg.mappingpedia.r2rml

class MappingPediaExecutionResult(val manifestText:String, val mappingText:String, val status:String, val errorCode:Integer) {
  def getManifestText() = manifestText;
  def getMappingText() = mappingText;
  def getStatus() = status;
  def getErrorCode() = errorCode;
}
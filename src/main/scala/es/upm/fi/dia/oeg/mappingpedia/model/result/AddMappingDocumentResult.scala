package es.upm.fi.dia.oeg.mappingpedia.model.result

class AddMappingDocumentResult(
                         val errorCode:Integer, val status:String
                         , val mappingURL:String
                         , val manifestURL:String

                         , val virtuosoStoreManifestStatus:String, val virtuosoStoreMappingStatus:String

                       ){

  def getErrorCode = this.errorCode;
  def getStatus = this.status;
  def getMappingURL = this.mappingURL;
  def getManifestURL = this.manifestURL;
  def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestStatus
  def getVirtuosoStoreMappingStatus = this.virtuosoStoreMappingStatus
}

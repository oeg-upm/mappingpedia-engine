package es.upm.fi.dia.oeg.mappingpedia.model.result

class AddMappingDocumentResult(
                         val errorCode:Integer, val status:String
                         , val mappingURL:String
                         , val manifestURL:String

                         , val virtuosoStoreManifestStatus:String, val virtuosoStoreMappingStatus:String

                       ){

  def getErrorCode = this.errorCode;
  def getStatus_code = this.errorCode;

  def getStatus = this.status;

  def getMappingURL = this.mappingURL;
  def getMapping_document_access_url= this.mappingURL;

  def getManifestURL = this.manifestURL;
  def getManifest_access_url = this.manifestURL;

  def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestStatus
  def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestStatus

  def getVirtuosoStoreMappingStatus = this.virtuosoStoreMappingStatus
  def getVirtuoso_store_mapping_status = this.virtuosoStoreMappingStatus
}

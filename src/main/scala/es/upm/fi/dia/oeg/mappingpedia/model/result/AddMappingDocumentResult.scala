package es.upm.fi.dia.oeg.mappingpedia.model.result

import es.upm.fi.dia.oeg.mappingpedia.model.MappingDocument

class AddMappingDocumentResult(val errorCode:Integer, val status:String
                               , val mappingDocument:MappingDocument
                               , val virtuosoStoreManifestStatus:String, val virtuosoStoreMappingStatus:String
                       ){

  def getErrorCode = this.errorCode;
  def getStatus_code = this.errorCode;

  def getStatus = this.status;

  def getMappingURL = mappingDocument.accessURL
  def getMapping_document_access_url= mappingDocument.accessURL
  def getMapping_document_download_url= mappingDocument.getDownloadURL()
  def getMapping_document_sha= mappingDocument.sha

  def getManifestURL = mappingDocument.manifestAccessURL
  def getManifest_access_url = mappingDocument.manifestAccessURL
  def getManifest_download_url = mappingDocument.manifestDownloadURL

  def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestStatus
  def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestStatus

  def getVirtuosoStoreMappingStatus = this.virtuosoStoreMappingStatus
  def getVirtuoso_store_mapping_status = this.virtuosoStoreMappingStatus

  def getId = mappingDocument.dctIdentifier
}

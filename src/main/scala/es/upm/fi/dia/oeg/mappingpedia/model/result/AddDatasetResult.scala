package es.upm.fi.dia.oeg.mappingpedia.model.result

import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution}

class AddDatasetResult(val errorCode:Integer, val status:String, val dataset:Dataset
                        , val storeManifestResponseStatus:Integer, val storeManifestResponseStatusText:String
                        , val storeDatasetResponseStatus:Integer, val storeDatasetResponseStatusText:String
                        , val virtuosoStoreManifestResponseStatusText:String
                        , val ckanStorePackageStatus:Integer, val ckanStoreResourceStatus:Integer
                      ) {


  def getStatus= this.status
  def getErrorCode = this.errorCode
  def getStatus_code = this.errorCode

  def getManifestURL = dataset.manifestAccessURL
  def getManifest_access_url = dataset.manifestAccessURL
  def getManifest_download_url = dataset.manifestDownloadURL


  //def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestResponseStatusText;
  def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestResponseStatusText;

  //def getCKANStorePackageStatus = this.ckanStorePackageStatus;
  def getCKAN_store_package_status = this.ckanStorePackageStatus;

  //def getCKANStoreResourceStatus = this.ckanStoreResourceStatus
  def getCKAN_store_resource_status = this.ckanStoreResourceStatus

  def getDatasetId = dataset.dctIdentifier
  def getDataset_id = dataset.dctIdentifier

  val distribution = dataset.getDistribution();
  def getDatasetURL = if(distribution == null ) { null } else { distribution.dcatAccessURL; }
  def getDistribution_access_url = if(distribution == null ) { null } else { distribution.dcatAccessURL }
  def getDistribution_download_url = if(distribution == null ) { null } else { distribution.dcatDownloadURL }
  def getDistribution_manifest_access_url = if(distribution == null ) { null } else { distribution.manifestAccessURL }
  def getDistribution_manifest_download_url = if(distribution == null ) { null } else { distribution.manifestDownloadURL }
  def getDistribution_sha = if(distribution == null ) { null } else { distribution.sha }
  def getDistribution_id = if(distribution == null ) { null } else { distribution.dctIdentifier}
  def getDistribution_ckan_resource_id = if(distribution == null ) { null } else { distribution.ckanResourceId}

}

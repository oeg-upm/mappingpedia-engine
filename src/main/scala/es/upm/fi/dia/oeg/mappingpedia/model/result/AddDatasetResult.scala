package es.upm.fi.dia.oeg.mappingpedia.model.result

import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution}

class AddDatasetResult(val errorCode:Integer, val status:String
                       , val dataset:Dataset
                       , val storeManifestResponseStatus:Integer, val storeManifestResponseStatusText:String
                       , val virtuosoStoreManifestResponseStatusText:String
                       , val ckanStorePackageStatus:Integer
                      ) {
  def getStatus= this.status
  def getErrorCode = this.errorCode
  def getStatus_code = this.errorCode

  def getDatasetId = dataset.dctIdentifier
  def getDataset_id = dataset.dctIdentifier
  def getLanding_page = dataset.dcatLandingPage;

  def getManifestURL = dataset.manifestAccessURL
  def getManifest_access_url = dataset.manifestAccessURL
  def getManifest_download_url = dataset.manifestDownloadURL

  def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestResponseStatusText;


  def getCKAN_store_package_status = this.ckanStorePackageStatus;

  val distribution = dataset.getDistribution();
  def getDatasetURL = if(distribution == null ) { null } else { distribution.dcatAccessURL; }
  def getDistribution_access_url = if(distribution == null ) { null } else { distribution.dcatAccessURL }
  def getDistribution_download_url = if(distribution == null ) { null } else { distribution.dcatDownloadURL }
  def getDistribution_manifest_access_url = if(distribution == null ) { null } else { distribution.manifestAccessURL }
  def getDistribution_manifest_download_url = if(distribution == null ) { null } else { distribution.manifestDownloadURL }
  def getDistribution_hash= if(distribution == null ) { null } else { distribution.hash }
  def getDistribution_id = if(distribution == null ) { null } else { distribution.dctIdentifier}
  def getDistribution_ckan_resource_id = if(distribution == null ) { null } else { distribution.ckanResourceId}
  def getCKAN_package_id = dataset.ckanPackageId

}

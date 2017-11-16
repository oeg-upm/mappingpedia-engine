package es.upm.fi.dia.oeg.mappingpedia.model.result

import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution}

class AddDatasetResult(val errorCode:Integer, val status:String, val dataset:Dataset
                        , val storeManifestResponseStatus:Integer, val storeManifestResponseStatusText:String
                        , val storeDatasetResponseStatus:Integer, val storeDatasetResponseStatusText:String
                        , val virtuosoStoreManifestResponseStatusText:String
                        , val ckanStorePackageStatus:Integer, val ckanStoreResourceStatus:Integer
                      ) {

  val distribution = dataset.getDistribution();

  def getStatus= this.status
  def getErrorCode = this.errorCode
  def getStatus_code = this.errorCode

  def getManifestURL = dataset.manifestAccessURL
  def getManifest_access_url = dataset.manifestAccessURL
  def getManifest_download_url = dataset.manifestDownloadURL

  def getDatasetURL = distribution.dcatAccessURL;
  def getDistribution_access_url = distribution.dcatAccessURL
  def getDistribution_download_url = distribution.dcatDownloadURL
  def getDistribution_manifest_access_url = distribution.manifestAccessURL
  def getDistribution_manifest_download_url = distribution.manifestDownloadURL
  def getDistribution_sha = distribution.sha

  //def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestResponseStatusText;
  def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestResponseStatusText;

  //def getCKANStorePackageStatus = this.ckanStorePackageStatus;
  def getCKAN_store_package_status = this.ckanStorePackageStatus;

  //def getCKANStoreResourceStatus = this.ckanStoreResourceStatus
  def getCKAN_store_resource_status = this.ckanStoreResourceStatus

  def getDatasetId = dataset.dctIdentifier
  def getDataset_id = dataset.dctIdentifier

  //def getDistributionId = this.ckanResourceId;
  def getDistribution_id = distribution.ckanResourceId
}

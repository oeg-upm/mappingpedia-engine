package es.upm.fi.dia.oeg.mappingpedia.model.result

class AddDatasetResult(
                        val errorCode:Integer, val status:String
                        , val manifestAccessURL:String
                        , val manifestDownloadURL:String
                        , val storeManifestResponseStatus:Integer
                        , val storeManifestResponseStatusText:String

                        , val distributionAccessURL:String
                        , val distributionDownloadURL:String
                        , val storeDatasetResponseStatus:Integer
                        , val storeDatasetResponseStatusText:String

                        , val virtuosoStoreManifestResponseStatusText:String

                        , val ckanStorePackageStatus:Integer
                        , val ckanStoreResourceStatus:Integer
                        , val ckanResourceId:String

                        , datasetID:String
                      ) {

  def getStatus= this.status
  def getErrorCode = this.errorCode
  def getStatus_code = this.errorCode

  def getManifestURL = this.manifestAccessURL;
  def getManifest_access_url = this.manifestAccessURL;
  def getManifest_download_url = this.manifestDownloadURL;

  def getDatasetURL = this.distributionAccessURL;
  def getDistribution_access_url = this.distributionAccessURL;
  def getDistribution_download_url = this.distributionDownloadURL

  def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestResponseStatusText;
  def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestResponseStatusText;

  def getCKANStorePackageStatus = this.ckanStorePackageStatus;
  def getCKAN_store_package_status = this.ckanStorePackageStatus;

  def getCKANStoreResourceStatus = this.ckanStoreResourceStatus
  def getCKAN_store_resource_status = this.ckanStoreResourceStatus

  def getDatasetId = this.datasetID;
  def getDataset_id = this.datasetID;

  def getDistributionId = this.ckanResourceId;
  def getDistribution_id = this.ckanResourceId;
}

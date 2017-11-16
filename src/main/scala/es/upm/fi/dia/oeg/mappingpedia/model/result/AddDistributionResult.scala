package es.upm.fi.dia.oeg.mappingpedia.model.result

class AddDistributionResult (
  val errorCode:Integer, val status:String

  , val manifestAccessURL:String, val manifestDownloadURL:String
  , val storeManifestResponseStatus:Integer , val storeManifestResponseStatusText:String

  , val distributionAccessURL:String, val distributionDownloadURL:String, val distributionSHA:String
  , val githubStoreDistributionResponseStatus:Integer, val githubStoreDistributionResponseStatusText:String

  , val virtuosoStoreManifestResponseStatusText:String

  , val ckanStoreResourceStatus:Integer, val ckanResourceId:String

  ) {

    def getStatus= this.status
    def getStatus_code = this.errorCode

    def getManifest_access_url = this.manifestAccessURL;
    def getManifest_download_url = this.manifestDownloadURL;

  def getGithubStoreDistributionResponseStatus = this.githubStoreDistributionResponseStatus
  def getGithubStoreDistributionResponseStatusText = this.githubStoreDistributionResponseStatusText
    def getDistribution_access_url = this.distributionAccessURL;
    def getDistribution_download_url = this.distributionDownloadURL
    def getDistribution_sha = this.distributionSHA

    //def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestResponseStatusText;
    def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestResponseStatusText;

    //def getCKANStoreResourceStatus = this.ckanStoreResourceStatus
    def getCKAN_store_resource_status = this.ckanStoreResourceStatus

    //def getDistributionId = this.ckanResourceId;
    def getDistribution_id = this.ckanResourceId;
}

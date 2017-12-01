package es.upm.fi.dia.oeg.mappingpedia.model.result

import es.upm.fi.dia.oeg.mappingpedia.model.Distribution

class AddDistributionResult (val errorCode:Integer, val status:String, distribution:Distribution
                             , val storeManifestResponseStatus:Integer , val storeManifestResponseStatusText:String
                             , val githubStoreDistributionResponseStatus:Integer, val githubStoreDistributionResponseStatusText:String
                             , val virtuosoStoreManifestResponseStatusText:String
                             , val ckanStoreResourceStatus:Integer
  ) {

    def getStatus= this.status
    def getStatus_code = this.errorCode

    def getManifest_access_url = distribution.manifestAccessURL
    def getManifest_download_url = distribution.manifestDownloadURL

  def getGithubStoreDistributionResponseStatus = this.githubStoreDistributionResponseStatus
  def getGithubStoreDistributionResponseStatusText = this.githubStoreDistributionResponseStatusText
    def getDistribution_access_url = distribution.dcatAccessURL
    def getDistribution_download_url = distribution.dcatDownloadURL
    def getDistribution_sha = distribution.sha

    //def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestResponseStatusText;
    def getVirtuoso_store_manifest_status = this.virtuosoStoreManifestResponseStatusText;

    //def getCKANStoreResourceStatus = this.ckanStoreResourceStatus
    def getCKAN_store_resource_status = this.ckanStoreResourceStatus

    //def getDistributionId = this.ckanResourceId;
    def getDistribution_id = distribution.dctIdentifier
    def getCKAN_resource_id = distribution.ckanResourceId

}

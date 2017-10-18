package es.upm.fi.dia.oeg.mappingpedia.model


class AddDatasetResult(
                        val errorCode:Integer, val status:String
                        , val gitHubManifestURL:String
                        , val gitHubStoreManifestResponseStatus:Integer
                        , val gitHubStoreManifestResponseStatusText:String

                       , val gitHubDatasetURL:String
                       , val gitHubStoreDatasetResponseStatus:Integer
                       , val gitHubStoreDatasetResponseStatusText:String

                       , val virtuosoStoreManifestResponseStatusText:String

                       , val ckanStorePackageStatus:String
                       , val ckanStoreResourceStatus:String
                      ) {

  def getStatus= this.status
  def getErrorCode = this.errorCode

  def getGitHubManifestURL = this.gitHubManifestURL;

  def getGitHubDatasetURL = this.gitHubDatasetURL;

  def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestResponseStatusText;

  def getCKANStorePackageStatus = this.ckanStorePackageStatus;
  def getCKANStoreResourceStatus = this.ckanStoreResourceStatus
}

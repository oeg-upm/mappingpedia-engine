package es.upm.fi.dia.oeg.mappingpedia.model.result

class AddDatasetResult(
                        val errorCode:Integer, val status:String
                        , val manifestURL:String
                        , val storeManifestResponseStatus:Integer
                        , val storeManifestResponseStatusText:String

                        , val datasetURL:String
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

  def getManifestURL = this.manifestURL;

  def getDatasetURL = this.datasetURL;

  def getVirtuosoStoreManifestStatus = this.virtuosoStoreManifestResponseStatusText;

  def getCKANStorePackageStatus = this.ckanStorePackageStatus;
  def getCKANStoreResourceStatus = this.ckanStoreResourceStatus

  def getDatasetId = this.datasetID;

  def getDistributionId = this.ckanResourceId;
}

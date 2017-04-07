package es.upm.fi.dia.oeg.mappingpedia.r2rml.model

/**
  * Created by fpriyatna on 07/04/2017.
  */
class MappingDocument(val id:String, val title:String, val dataset:String, val filePath:String, val creator:String
                      , val distribution:String, val distributionAccessURL:String) {
  def getId = this.id;
  def getTitle = this.title;
  def getDataset = this.dataset;
  def getCreator = this.creator;
  //def getDistribution = this.distribution;
  def getDistributionAccessURL = this.distributionAccessURL;
}

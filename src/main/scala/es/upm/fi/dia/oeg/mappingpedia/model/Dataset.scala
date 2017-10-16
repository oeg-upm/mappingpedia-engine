package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

//based on dcat:Dataset
//see also ckan:Package
class Dataset(val dctIdentifier:String){
  def this() {
    this(UUID.randomUUID.toString)
  }

  var dctTitle:String = null;
  var dctDescription:String = null;
  var dctLanguage:String = null;
  var dcatKeyword:String = null;
  var dcatDistribution:List[Distribution] = Nil;

  //for the moment assume that only one distribution for each dataset
  def getDistribution() = this.dcatDistribution.iterator.next()

  def addDistribution(distribution: Distribution) = {
    this.dcatDistribution = distribution :: this.dcatDistribution;
  }
}

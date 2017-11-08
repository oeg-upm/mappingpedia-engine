package es.upm.fi.dia.oeg.mappingpedia.model

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

//based on dcat:Dataset
//see also ckan:Package
class Dataset(val dctPublisher:Organization, val dctIdentifier:String){
  val createdDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())

  def this(dctPublisher:Organization) {
    this(dctPublisher, UUID.randomUUID.toString)
  }

  var dctTitle:String = null;
  var dctDescription:String = null;
  var dctIssued:String = createdDate;
  var dctModified:String = createdDate;
  var dcatKeyword:String = null;
  var dctLanguage:String = null;
  var dcatDistribution:List[Distribution] = Nil;

  //for the moment assume that only one distribution for each dataset
  def getDistribution() = this.dcatDistribution.iterator.next()

  def addDistribution(distribution: Distribution) = {
    if(this.dcatDistribution == null) {
      this.dcatDistribution = List(distribution);
    } else {
      if(!this.dcatDistribution.contains(distribution)) {
        this.dcatDistribution = distribution :: this.dcatDistribution;
      }
    }
  }
}

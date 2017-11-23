package es.upm.fi.dia.oeg.mappingpedia.model

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import org.slf4j.{Logger, LoggerFactory}
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine.sdf

//based on dcat:Disctribution https://www.w3.org/TR/vocab-dcat/#class-distribution
//see also ckan:Resource  http://docs.ckan.org/en/ckan-1.7.4/domain-model-resource.html
class Distribution (val dataset: Dataset, val dctIdentifier:String){
  def this(dataset: Dataset) {
    this(dataset, UUID.randomUUID.toString)
  }


  val createdDate = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date())
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  dataset.addDistribution(this);

  //FIELDS FROM DCAT
  var dctTitle:String = null;
  var dctDescription:String = null;
  var dctIssued:String = createdDate;
  var dctModified:String = createdDate;
  var dcatAccessURL:String = null;
  var dcatDownloadURL:String = null;
  var dcatMediaType:String = null;

  //CUSTOM FIELDS
  var cvsFieldSeparator:String=",";
  var distributionFile:File = null;
  var encoding:String="UTF-8";
  var ckanResourceId:String = null;
  var sha:String = null;
  var manifestAccessURL:String = null;
  var manifestDownloadURL:String = null;


  def getId = this.dctIdentifier
  def getAccess_url = this.dcatAccessURL;
  def getDownload_url = this.dcatDownloadURL;
}

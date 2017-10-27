package es.upm.fi.dia.oeg.mappingpedia.model

import java.io.File

import org.springframework.web.multipart.MultipartFile

//based on dcat:Disctribution
//see also ckan:Resource
class Distribution (val dataset: Dataset){
  dataset.addDistribution(this);

  //FIELDS FROM DCAT
  var dctTitle:String = null;
  var dctDescription:String = null;
  var dcatAccessURL:String = null;
  var dcatDownloadURL:String = null;
  var dcatMediaType:String = null;

  //FIELDS FROM CKAN
  var ckanFileRef:MultipartFile = null;

  //CUSTOM FIELDS
  var cvsFieldSeparator:String=",";
  var distributionFile:File = null;
}

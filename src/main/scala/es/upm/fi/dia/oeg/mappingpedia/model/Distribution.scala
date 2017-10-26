package es.upm.fi.dia.oeg.mappingpedia.model

import java.io.File

import org.springframework.web.multipart.MultipartFile

//based on dcat:Disctribution
//see also ckan:Resource
class Distribution (val dataset: Dataset){
  if(dataset.dcatDistribution == null) {
    dataset.dcatDistribution = Nil;
  }
  dataset.addDistribution(this);
  var dcatAccessURL:String = null;
  var dcatDownloadURL:String = null;
  var dcatMediaType:String = null;
  var ckanFileRef:MultipartFile = null; // This is not used at the moment because ckan always do to the download url
  var ckanDescription:String = null;

  var cvsFieldSeparator:String=",";
  var distributionFile:File = null;
}

package es.upm.fi.dia.oeg.mappingpedia.model

import org.springframework.web.multipart.MultipartFile

//based on dcat:Disctribution
//see also ckan:Resource
class Distribution (val dataset: Dataset){
  var dcatAccessURL:String = null;
  var dcatDownloadURL:String = null;
  var dcatMediaType:String = null;
  var ckanFileRef:MultipartFile = null;
  var ckanDescription:String = null;

}

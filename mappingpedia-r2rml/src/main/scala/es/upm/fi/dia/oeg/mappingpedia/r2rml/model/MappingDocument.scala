package es.upm.fi.dia.oeg.mappingpedia.r2rml.model

/**
  * Created by fpriyatna on 07/04/2017.
  */
class MappingDocument(val id:String, val title:String, val dataset:String, val filePath:String) {
  def getId = this.id;
  def getTitle = this.title;
  def getDataset = this.dataset;
  //def getFilePath = this.filePath;

}

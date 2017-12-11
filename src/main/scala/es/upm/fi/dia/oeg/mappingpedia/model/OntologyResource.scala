package es.upm.fi.dia.oeg.mappingpedia.model

/**
  * Created by fpriyatna on 11/12/17.
  */
class OntologyResource (val uri: String, val localName:String, val label:String, val comment:String) {

  def getURI = this.uri;
  def getLocal_name = this.localName;
  def getLabel = this.label;
  def getComment = this.comment
}

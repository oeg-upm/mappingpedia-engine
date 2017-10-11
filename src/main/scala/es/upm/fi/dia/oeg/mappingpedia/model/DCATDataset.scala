package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

class DCATDataset(val dctIdentifier:String){
  def this() {
    this(UUID.randomUUID.toString)
  }

  var dctTitle:String = null;
  var dctDescription:String = null;
  var dctKeyword:String = null;

}

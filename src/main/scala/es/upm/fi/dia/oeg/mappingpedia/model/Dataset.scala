package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

class Dataset (val identifier:String){
  def this() {
    this(UUID.randomUUID.toString)
  }

  var title:String = null;
  var description:String = null;
  var keywords:String = null;

}

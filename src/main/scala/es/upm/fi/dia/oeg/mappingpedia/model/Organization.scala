package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

class Organization(val dctIdentifier:String) {
  def this() = {
    this(UUID.randomUUID.toString)
  }

  var foafName:String = null;

}

package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

class Agent(val dctIdentifier:String) {
  def this() = {
    this(UUID.randomUUID.toString)
  }

  var foafName:String = null;
  var foafMbox:String = null;

}

object Agent {
  def apply(name:String, email:String) = {
    val newAgent:Agent = new Agent();
    newAgent.foafName = name;
    newAgent.foafMbox = email;
    newAgent
  }
}

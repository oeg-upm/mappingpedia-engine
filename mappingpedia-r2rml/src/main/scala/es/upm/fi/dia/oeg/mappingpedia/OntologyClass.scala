package es.upm.fi.dia.oeg.mappingpedia

import es.upm.fi.dia.oeg.mappingpedia.r2rml.MappingPediaUtility

/**
  * Created by fpriyatna on 6/7/17.
  */
class OntologyClass (val aClass:String, val superClass:String, val superclasses:List[String]) {
  def getAClass = this.aClass;
  //def getSuperClass = this.superClass;
  def getSuperclasses = this.superclasses.mkString(",")

  override def toString: String = this.aClass + " - " + this.superclasses.mkString;
}

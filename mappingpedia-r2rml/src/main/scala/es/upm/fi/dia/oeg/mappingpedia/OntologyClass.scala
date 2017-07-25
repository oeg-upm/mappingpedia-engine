package es.upm.fi.dia.oeg.mappingpedia

import es.upm.fi.dia.oeg.mappingpedia.r2rml.MappingPediaUtility
import collection.JavaConverters._

/**
  * Created by fpriyatna on 6/7/17.
  */
class OntologyClass (val aClass:String, val superClasses:List[String], val subClasses:List[String]) {
  def getAClass = this.aClass;
  def getSuperClasses = this.superClasses.mkString(",")
  def getSubClasses = this.subClasses.mkString(",")
  def getSubClassesList = this.subClasses.asJava
  def getSuperClassesList = this.superClasses.asJava

}

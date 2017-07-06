package es.upm.fi.dia.oeg.mappingpedia

/**
  * Created by fpriyatna on 6/7/17.
  */
class OntologyClass (val aClass:String, val superClass:String, val superclasses:List[String]) {
  def getAClass = this.aClass;
  def getSuperClass = this.superClass;
  def getSuperclasses = this.superclasses;

  override def toString: String = this.aClass + " - " + this.superClass + " - " + this.superclasses.mkString;
}

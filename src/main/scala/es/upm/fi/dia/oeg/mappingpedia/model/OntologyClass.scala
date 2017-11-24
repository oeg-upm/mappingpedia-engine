package es.upm.fi.dia.oeg.mappingpedia.model

import org.apache.jena.enhanced.EnhGraph
import org.apache.jena.ontology.OntClass
import org.apache.jena.ontology.impl.OntClassImpl
import org.apache.jena.vocabulary.RDFS

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
/**
  * Created by fpriyatna on 6/7/17.
  */
class OntologyClass (val ontClass:OntClass, val clsSuperclasses:List[OntClass], val clsSubClasses:List[OntClass]){

  def getLocalName = ontClass.getLocalName;
  def getURI = ontClass.getURI
  def getDescription = ontClass.getPropertyValue(RDFS.comment).toString

  def getSuperClassesLocalNames = this.clsSuperclasses.map(superClass => superClass.getLocalName).mkString(",")
  def getSuperClassesURIs = this.clsSuperclasses.map(superClass => superClass.getURI).mkString(",")

  def getSubClassesLocalNames = this.clsSubClasses.map(subClass => subClass.getLocalName).mkString(",")
  def getSubClassesURIs = this.clsSubClasses.map(subClass => subClass.getURI).mkString(",")


}

package es.upm.fi.dia.oeg.mappingpedia.r2rml

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by fpriyatna on 04/04/2017.
  */
class ListResult (val count:Integer, val results:List[Object]) {
  def getCount() = this.count;

  def getResults() = this.results.asJava;



}

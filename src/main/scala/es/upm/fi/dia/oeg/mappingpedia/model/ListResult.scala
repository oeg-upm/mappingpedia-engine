package es.upm.fi.dia.oeg.mappingpedia.model

import scala.collection.JavaConverters._

/**
  * Created by fpriyatna on 04/04/2017.
  */
class ListResult (val count:Integer, val results:Iterable[Object]) {
  def getCount() = this.count;

  def getResults() = this.results.asJava;


  override def toString = s"ListResult($getCount, $getResults)"
}

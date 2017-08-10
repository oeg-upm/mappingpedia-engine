package es.upm.fi.dia.oeg.mappingpedia.model

import scala.collection.JavaConverters._


/**
  * Created by fpriyatna on 6/7/17.
  */
class MapResult (val count:Integer, val results:Map[String, List[String]]) {
  def getCount() = this.count;

  def getResults() = this.results.asJava;


}

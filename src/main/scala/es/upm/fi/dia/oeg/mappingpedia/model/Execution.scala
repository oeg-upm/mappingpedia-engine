package es.upm.fi.dia.oeg.mappingpedia.model

class Execution (val mappingURL:String,
  val datasetURL:String,
  val executionURL:String)
{
  def getDatasetURL = this.datasetURL;
  def getMappingURL = this.mappingURL;
  def getExecutionURL = this.executionURL
}


package es.upm.fi.dia.oeg.mappingpedia.model

class Execution (
                  //val errorCode:Integer, val status:String
                  val mappingURL:String,
                  val datasetURL:String,
                  val mappingExecutionResultURL:String, val mappingExecutionResultDownloadURL:String
                )
{
  def getDatasetURL = this.datasetURL;
  def getMappingURL = this.mappingURL;
  def getMappingExecutionResultURL = this.mappingExecutionResultURL
  def getMappingExecutionResultDownloadURL = this.mappingExecutionResultDownloadURL
}


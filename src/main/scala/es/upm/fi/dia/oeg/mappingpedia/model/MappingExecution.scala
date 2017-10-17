package es.upm.fi.dia.oeg.mappingpedia.model

class MappingExecution(val mappingDocument:MappingDocument, val dataset: Dataset) {
  //val dataset: Dataset = null;
  var outputFileName = "output.txt";
  var queryFilePath:String = null;
  var storeToCKAN:Boolean = true;

  def setStoreToCKAN(pStoreToCKAN:String) = {
    if(pStoreToCKAN != null) {
      if("true".equalsIgnoreCase(pStoreToCKAN) || "yes".equalsIgnoreCase(pStoreToCKAN)) {
        storeToCKAN = true;
      }
    }

  }

}

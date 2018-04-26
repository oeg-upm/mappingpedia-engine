package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

class MappingExecution(val mappingDocument:MappingDocument
                       , val unannotatedDistributions: List[UnannotatedDistribution]
                       , val jdbcConnection: JDBCConnection
                       , val queryFileName:String
                       , val pOutputFileName:String
                       , val pOutputFileExtension:String
                       , val pOutputMediaType: String
                       , val pStoreToCKAN:Boolean
                       , val pStoreToGithub:Boolean
                       , val pStoreExecutionResultToVirtuoso:Boolean
                       , val useCache:Boolean
                       , val callbackURL:String
                       , val updateResource:Boolean
                      ) {
  /*
    def this(mappingDocument:MappingDocument
             , unannotatedDistributions: List[UnannotatedDistribution]
             , queryFileName:String
             , pOutputFileDirectory:String
             , pOutputFilename:String
             , pOutputFileExtension:String
            ) {
      this(mappingDocument, unannotatedDistributions, null, queryFileName
        , pOutputFileDirectory, pOutputFilename, pOutputFileExtension);
    }

    def this(mappingDocument:MappingDocument
             , jdbcConnection: JDBCConnection
             , queryFileName:String
             , pOutputFileDirectory:String
             , pOutputFilename:String
             , pOutputFileExtension:String
            ) {
      this(mappingDocument, null, jdbcConnection, queryFileName
        , pOutputFileDirectory, pOutputFilename, pOutputFileExtension);
    }
  */

  //val dataset: Dataset = null;
  //var outputFileName = "output.txt";
  var outputDirectory:String = null;

  var storeToCKAN:Boolean = true;

  val outputFileName = if (pOutputFileName == null) {
    UUID.randomUUID.toString
  } else {
    pOutputFileName;
  }
  val outputFileExtension = if(pOutputFileExtension == null) {
    "txt"
  } else {
    pOutputFileExtension
  }

  def getOutputFileWithExtension = s"${outputFileName}.${outputFileExtension}";

}

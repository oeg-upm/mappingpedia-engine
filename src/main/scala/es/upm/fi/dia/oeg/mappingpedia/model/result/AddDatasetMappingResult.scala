package es.upm.fi.dia.oeg.mappingpedia.model.result

/**
  * Created by fpriyatna on 24/11/17.
  */
class AddDatasetMappingExecuteResult (val statusCode:Int,
                                      val addDatasetResult:AddDatasetResult
                                      , val addMappingDocumentResult: AddMappingDocumentResult
                                      , val executeMappingResult: ExecuteMappingResult) {
  def getStatus_code = this.statusCode
  def getAdd_dataset_result = this.addDatasetResult;
  def getAdd_mapping_document_result = this.addMappingDocumentResult;
  def getExecute_mapping_result = this.executeMappingResult;

}

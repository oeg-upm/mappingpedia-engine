package es.upm.fi.dia.oeg.mappingpedia.model.result

/**
  * Created by fpriyatna on 24/11/17.
  */
class AddDatasetMappingExecuteResult (val addDatasetResult:AddDatasetResult
                                      , val addMappingDocumentResult: AddMappingDocumentResult
                                      , val executeMappingResult: ExecuteMappingResult) {
  def getAddDatasetResult() = this.addDatasetResult;
  def getAddMappingDocumentResult = this.addMappingDocumentResult;

}

package es.upm.fi.dia.oeg.mappingpedia.utility

import es.upm.fi.dia.oeg.mappingpedia.model.OntologyClass

/**
  * Created by fpriyatna on 6/7/17.
  */
object MappingPediaUtilityTest {
  val model = MappingPediaUtility.loadSchemaOrgOntology();

  val aClass = "MedicalBusiness"
  val subclasses = MappingPediaUtility.getSubclassesSummary(aClass, model, "0", "0");
  for(subclass <- subclasses.results) {
    println("subclass = " + subclass)
  }

}

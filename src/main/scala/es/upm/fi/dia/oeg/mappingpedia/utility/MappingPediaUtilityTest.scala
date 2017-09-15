package es.upm.fi.dia.oeg.mappingpedia.utility

import es.upm.fi.dia.oeg.mappingpedia.Application
import es.upm.fi.dia.oeg.mappingpedia.model.OntologyClass

/**
  * Created by fpriyatna on 6/7/17.
  */
object MappingPediaUtilityTest {
  val app:Application = new Application;

  val model = MappingPediaUtility.loadSchemaOrgOntology();

  val aClass = "MedicalBusiness"
  val subclasses = MappingPediaUtility.getSubclassesLocalNames(aClass, model, "0", "0");
  println("subclasses = " + subclasses)

}

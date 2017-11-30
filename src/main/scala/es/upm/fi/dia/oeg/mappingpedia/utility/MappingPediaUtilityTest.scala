package es.upm.fi.dia.oeg.mappingpedia.utility

import es.upm.fi.dia.oeg.mappingpedia.Application
import es.upm.fi.dia.oeg.mappingpedia.model.OntologyClass

/**
  * Created by fpriyatna on 6/7/17.
  */
object MappingPediaUtilityTest {
  val app:Application = new Application;

  val model = JenaClient.loadSchemaOrgOntology("tree.jsonld", "JSON-LD");
  val jenaClient = new JenaClient(model)

  val aClass = "MedicalBusiness"
  val subclasses = jenaClient.getSubclassesLocalNames(aClass);
  println("subclasses = " + subclasses)

}

package es.upm.fi.dia.oeg.mappingpedia.r2rml

/**
  * Created by fpriyatna on 6/7/17.
  */
object MappingPediaUtilityTest extends App {
  val model = MappingPediaUtility.loadSchemaOrgOntology();

  val aClass = "MedicalBusiness"
  val subclasses = MappingPediaUtility.getSubclasses(aClass, model, "0", "0");
  for(subclass <- subclasses.results) {
    println("subclass = " + subclass)
  }

}

package es.upm.fi.dia.oeg.mappingpedia.model

//based on prov:Entity https://www.w3.org/TR/prov-o/#Entity
trait Entity {
  //FIELDS FROM PROV-O
  var provWasAttributedTo:String = null;
  var provWasGeneratedBy:String = null;
  var provWasDerivedFrom:String = null;
  var provSpecializationOf:String = null;
  var provHadPrimarySource:String = null;
  var provWasRevisionOf:String = null;
  var provWasInfluencedBy:String = null;

}

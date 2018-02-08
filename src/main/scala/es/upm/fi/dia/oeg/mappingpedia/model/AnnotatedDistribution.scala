package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

class AnnotatedDistribution (dataset: Dataset, dctIdentifier:String)
  extends Distribution(dataset, dctIdentifier) {

  def this(dataset: Dataset) {
    this(dataset, UUID.randomUUID.toString)
  }
}

object AnnotatedDistribution {
  def apply(dataset: Dataset, dctIdentifier:String) = {
    if(dctIdentifier == null) {
      new AnnotatedDistribution(dataset)
    } else {
      new AnnotatedDistribution(dataset, dctIdentifier)
    }
  }
}

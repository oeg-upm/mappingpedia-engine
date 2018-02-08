package es.upm.fi.dia.oeg.mappingpedia.model

import java.util.UUID

class UnannotatedDistribution (dataset: Dataset, dctIdentifier:String)
  extends Distribution(dataset, dctIdentifier) {
  def this(dataset: Dataset) {
    this(dataset, UUID.randomUUID.toString)
  }
}

object UnannotatedDistribution {
  def apply(dataset: Dataset, dctIdentifier:String) = {
    if(dctIdentifier == null) {
      new UnannotatedDistribution(dataset)
    } else {
      new UnannotatedDistribution(dataset, dctIdentifier)
    }
  }
}
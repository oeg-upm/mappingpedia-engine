package es.upm.fi.dia.oeg.mappingpedia.test

import es.upm.fi.dia.oeg.mappingpedia.MappingPediaProperties
import es.upm.fi.dia.oeg.mappingpedia.MappingPediaEngine
import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController
import es.upm.fi.dia.oeg.mappingpedia.model.Dataset
import org.slf4j.{Logger, LoggerFactory}
import com.mashape.unirest.http.Unirest

object TestDatasetsControllers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def main(args:Array[String]) = {
    val properties:MappingPediaProperties = MappingPediaProperties.apply();
    MappingPediaEngine.init(properties);
    val datasetsController = new DatasetController(
      MappingPediaEngine.ckanClient
      , MappingPediaEngine.githubClient
      , MappingPediaEngine.virtuosoClient
    );
    val organizationId = "test-upm";
    val dataset = Dataset.apply(organizationId, null);
    logger.info(s"dataset.dctIdentifier = ${dataset.dctIdentifier}");
    logger.info(s"dataset.dctPublisher.dctIdentifier = ${dataset.dctPublisher.dctIdentifier}");
    this.testFindOrCreate(datasetsController, dataset);
    //this.testPackageCreate(dataset);

    logger.info("Bye");
  }

  def testFindOrCreate(datasetsController:DatasetController, dataset:Dataset) = {
    datasetsController.findOrCreate(
      dataset.dctPublisher.dctIdentifier
      , dataset.dctIdentifier
      , dataset.ckanPackageId
      , dataset.ckanPackageName
    );
  }

  def testPackageCreate(dataset:Dataset) {
    val response = Unirest.post("")
      .header("Authorization", "")
      .field("name", dataset.dctIdentifier)
      .field("owner_org", dataset.dctPublisher.dctIdentifier)
      .asJson();
    val responseStatus = response.getStatus;
    val responseStatusText = response.getStatusText
    logger.info(s"responseStatus = ${responseStatus}");
    logger.info(s"responseStatusText = ${responseStatusText}");
  }
}
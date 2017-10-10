package es.upm.fi.dia.oeg.mappingpedia.utility

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaEngine, MappingPediaProperties}
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}

class CKANUtility {

}

object CKANUtility {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def addNewOrganization(organizationName:String) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", organizationName);

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionOrganizationCreate
    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
    response;
  }

  def addNewDataset(datasetId:String, organizationId:String, datasetTitle:String) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", datasetId);
    jsonObj.put("owner_org", organizationId);
    jsonObj.put("title", datasetId);

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageCreate

    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
/*
    logger.info(s"response.getHeaders = ${response.getHeaders}")
    logger.info(s"response.getHeaders = ${response.getBody}")
    logger.info(s"response.getStatus = ${response.getStatus}")
*/

    response;
  }

  def addNewDistribution(packageId:String, description:Option[String], mimetype:Option[String]) = {
    val jsonObj = new JSONObject();
    jsonObj.put("package_id", packageId);
    if(description != null && description.isDefined) {
      jsonObj.put("description", description.get);
    }
    if(mimetype!= null && mimetype.isDefined) {
      jsonObj.put("mimetype", mimetype.get);
    }

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionResourceCreate
    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
    response;
  }

}

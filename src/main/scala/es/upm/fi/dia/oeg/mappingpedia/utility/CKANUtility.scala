package es.upm.fi.dia.oeg.mappingpedia.utility

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaEngine, MappingPediaProperties}
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

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

  def addNewPackage(packageId:String, organizationId:String, datasetTitle:String) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", packageId);
    jsonObj.put("owner_org", organizationId);
    if(datasetTitle != null) {
      jsonObj.put("title", datasetTitle);
    }


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

  def addNewResource(packageId:String, description:String, mimetype:String, datasetFileRef: MultipartFile) = {
    val jsonObj = new JSONObject();
    jsonObj.put("package_id", packageId);
    if(description != null) {
      jsonObj.put("description", description);
    }
    if(mimetype!= null ) {
      jsonObj.put("mimetype", mimetype);
    }
    if(mimetype!= null ) {
      jsonObj.put("upload", datasetFileRef);
    }

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionResourceCreate
    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
    response;
  }

}

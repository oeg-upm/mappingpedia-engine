package es.upm.fi.dia.oeg.mappingpedia.utility

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.model.{Dataset, Distribution, Organization}
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaEngine, MappingPediaProperties}
import org.json.JSONObject
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.web.multipart.MultipartFile

class CKANUtility {

}

object CKANUtility {
  val logger: Logger = LoggerFactory.getLogger(this.getClass);

  def addNewOrganization(organization:Organization) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", organization.foafName);

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionOrganizationCreate
    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
    response;
  }

  def addNewPackage(organization:Organization, dataset:Dataset) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", dataset.dctIdentifier);
    jsonObj.put("owner_org", organization.dctIdentifier);
    if(dataset.dctTitle != null) {
      jsonObj.put("title", dataset.dctTitle);
    }
    if(dataset.dctDescription != null) {
      jsonObj.put("notes", dataset.dctDescription);
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

  def addNewResource(dataset:Dataset, distribution:Distribution) = {
    val jsonObj = new JSONObject();
    jsonObj.put("package_id", dataset.dctIdentifier);
    jsonObj.put("url", distribution.dcatDownloadURL);
    if(distribution.ckanDescription != null) {
      jsonObj.put("description", distribution.ckanDescription);
    }
    if(distribution.dcatMediaType != null ) {
      jsonObj.put("mimetype", distribution.dcatMediaType);
    }
    if(distribution.ckanFileRef != null ) {
      jsonObj.put("upload", distribution.ckanFileRef);
    }

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionResourceCreate
    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
    response;
  }

}

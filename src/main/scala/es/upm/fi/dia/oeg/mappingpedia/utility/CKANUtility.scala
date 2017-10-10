package es.upm.fi.dia.oeg.mappingpedia.utility

import com.mashape.unirest.http.Unirest
import es.upm.fi.dia.oeg.mappingpedia.{MappingPediaEngine, MappingPediaProperties}
import org.json.JSONObject

class CKANUtility {

}

object CKANUtility {
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

  def addNewDataset(datasetName:String, organizationId:String) = {
    val jsonObj = new JSONObject();
    jsonObj.put("name", datasetName);
    jsonObj.put("owner_org", organizationId);

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageCreate
    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
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

    val uri = MappingPediaEngine.mappingpediaProperties.ckanActionPackageCreate
    val response = Unirest.post(uri)
      .header("Authorization", MappingPediaEngine.mappingpediaProperties.ckanKey)
      .body(jsonObj)
      .asJson();
    response;
  }
}

package es.upm.fi.dia.oeg.mappingpedia.r2rml

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.mashape.unirest.http.Unirest
import org.apache.logging.log4j.{LogManager, Logger}
import org.json.JSONObject

/**
  * Created by fpriyatna on 21/02/2017.
  */
class GitHubUtility {

}

object GitHubUtility {
  val logger : Logger = LogManager.getLogger("GitHubUtility");

  def putEncodedFile(uuid:String, filename:String, message:String, base64EncodedContent:String
              , githubUsername:String, githubAccessToken:String, mappingpediaUsername:String) = {
    logger.info("uuid = " + uuid);
    logger.info("filename = " + filename);

    val jsonObj = new JSONObject();
    jsonObj.put("message", message);
    jsonObj.put("content", base64EncodedContent);
    //jsonObj.put("sha", "fb617c9e42866ca24d0ff8e0c2725048f6f9530c");
    jsonObj.put("path", filename);
    //jsonObj.put("access_token", "7dabca6745186161c07d04353b6967249095b679");

    val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/{mappingpediaUsername}/{uuid}/{filename}";
    val response = Unirest.put(uri)
      .routeParam("mappingpediaUsername", mappingpediaUsername)
      .routeParam("uuid", uuid)
      .routeParam("filename", filename)
      .basicAuth(githubUsername, githubAccessToken)
      //.header("Content-Type", "application/json")
      .body(jsonObj)
      .asJson();
    //logger.info("responseHeaders = " + response.getHeaders);
    //logger.info("responseBody = " + response.getBody);
    response;
  }

  def encodeToBase64(content:String) : String = {
    val base64EncodedContent = BaseEncoding.base64().encode(content.getBytes(Charsets.UTF_8));
    base64EncodedContent;
  }
}

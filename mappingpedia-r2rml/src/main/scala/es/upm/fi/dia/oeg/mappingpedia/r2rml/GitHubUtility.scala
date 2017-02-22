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

  def putEncodedFile(mappingDirectory:String, mappingFilename:String, message:String, base64EncodedContent:String
    , githubUsername:String, githubAccessToken:String, mappingpediaUsername:String
    //, sha:Option[String]
  ) = {
    /*
    logger.info("content= " + base64EncodedContent);
    logger.info("message = " + message);
    logger.info("sha = " + sha);
    logger.info("mappingpediaUsername= " + mappingpediaUsername);
    logger.info("uuid = " + uuid);
    logger.info("filename = " + filename);
    */

    val jsonObj = new JSONObject();
    jsonObj.put("message", message);
    jsonObj.put("content", base64EncodedContent);

    try {
      val sha = GitHubUtility.getSHA(mappingpediaUsername, mappingDirectory, mappingFilename
        , Application.prop.githubUser, Application.prop.githubAccessToken);
      jsonObj.put("sha", sha);
    } catch {
      case e:Exception => {}
    }

    //if(sha!= null && sha.isDefined) { jsonObj.put("sha", sha.get); }
    //jsonObj.put("path", filename);
    //jsonObj.put("access_token", "7dabca6745186161c07d04353b6967249095b679");

    //val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/mappingpedia-testuser/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c.ttl";
    val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val response = Unirest.put(uri)
      .routeParam("mappingpediaUsername", mappingpediaUsername)
      .routeParam("mappingDirectory", mappingDirectory)
      .routeParam("mappingFilename", mappingFilename)
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

  def getSHA(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
             , githubUsername:String, githubAccessToken:String) : String = {
    val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val response = Unirest.get(uri)
      .routeParam("mappingpediaUsername", mappingpediaUsername)
      .routeParam("mappingDirectory", mappingDirectory)
      .routeParam("mappingFilename", mappingFilename)
      //.routeParam("mappingFileExtension", mappingFileExtension)
      .basicAuth(githubUsername, githubAccessToken)
      .asJson();

    response.getBody.getObject.getString("sha");
  }

  def getSHA(url:String, githubUsername:String, githubAccessToken:String) : String = {
    val response = Unirest.get(url)
      .basicAuth(githubUsername, githubAccessToken)
      .asJson();
    response.getBody.getObject.getString("sha");
  }
}

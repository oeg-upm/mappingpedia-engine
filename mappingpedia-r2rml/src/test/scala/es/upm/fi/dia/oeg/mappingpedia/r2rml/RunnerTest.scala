package es.upm.fi.dia.oeg.mappingpedia.r2rml
import scala.io.Source._

object RunnerTest {
  val VIRTUOSO_USER : String = "...";
  val VIRTUOSO_JDBC : String = "...";
  val VIRTUOSO_PWD : String = "...";
  
  val MANIFEST_FILE_PATH : String = "manifest.ttl";
	
	def main(args: Array[String]): Unit = {
	  
	  //Runner.run("http://mappingpedia.linkeddata.es/graph/data");
	  
	  
	  val manifestText = fromFile("manifest.ttl").getLines.mkString("\n");
	  val mappingText = fromFile("mapping.ttl").getLines.mkString("\n");
	  
	  println("manifestText = " + manifestText);
	  println("mappingText = " + mappingText);
	  
	  val virtuosoJDBC = "...";
	  val virtuosoUser = "...";
	  val virtuosoPwd = "...";
	  val graphName = "...";
	  val clearGraph = false;
	  
	  Runner.runManifestAndMappingInString(manifestText, mappingText
	      , virtuosoJDBC, virtuosoUser, virtuosoPwd, graphName, clearGraph);
	  println("Done");
	}
	  
	  
}
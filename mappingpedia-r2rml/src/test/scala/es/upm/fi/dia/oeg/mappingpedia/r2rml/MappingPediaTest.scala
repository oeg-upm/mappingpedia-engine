package es.upm.fi.dia.oeg.mappingpedia.r2rml

object MappingPediaTest {
  val VIRTUOSO_USER : String = "...";
  val VIRTUOSO_JDBC : String = "...";
  val VIRTUOSO_PWD : String = "...";
  
  val MANIFEST_FILE_PATH : String = "manifest.ttl";
	
	def main(args: Array[String]): Unit = {
	  
	  Runner.run(MANIFEST_FILE_PATH, VIRTUOSO_JDBC
	      , VIRTUOSO_USER, VIRTUOSO_PWD, "true");
	}
	  
	  
}
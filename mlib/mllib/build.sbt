libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.6.1"

lazy val download = taskKey[Unit]("Download resources and extract them to resources/ folders.")

def urlZIP(url: String) {
  println("Downloading " + url)
  IO.unzipURL(new URL(url),new File("temp"))
}

def move(in: String, out: String) {
  println("moving " + in)
  IO.move(new File(in),new File("src/main/resources/"+out))
}

download := {
  if(java.nio.file.Files.notExists(new File("src/main/resources/").toPath())) {
    //download
    println("Downloading resources...")
    urlZIP("http://cs.hut.fi/u/arasalo1/resources/osge_pool-1-thread-1.data.zip")

    //rename and remove unnecessary files
    move("temp/osge_pool-1-thread-1.data", "osge_pool-1-thread-1.data")
    IO.delete(List(new File("temp/")))

  } else {
    println("Resources already downloaded. If you want to download again "+
            "remove folder src/main/resources/.")
  }
}
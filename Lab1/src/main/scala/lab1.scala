import scala.io.Source
import io.circe.syntax._
import java.io._

object lab1 extends App {
  def Read(path: String, myFilmID: Int): (List[Int], List[Int]) = {
    val file = Source.fromResource(path).getLines
    val oneFilm: Array[Int] = Array(0, 0, 0, 0, 0)
    val allFilms: Array[Int] = Array(0, 0, 0, 0, 0)

    for (line <- file) {
      val splitLine = line.split("\t").toList.map(x => x.toInt)

      if (splitLine(1) == myFilmID) {
        oneFilm(splitLine(2) - 1) += 1
      }
      allFilms(splitLine(2) - 1) += 1
    }

    (oneFilm.toList, allFilms.toList)
  }

  def Write(myFilmMarks: List[Int], allFilmsMarks: List[Int],
            fieldNameMyFilm: String, fieldNameAllFilms: String, outputName: String): Unit = {
    val result = Map(
      fieldNameMyFilm -> myFilmMarks.asJson,
      fieldNameAllFilms -> allFilmsMarks.asJson
    ).asJson
    val writer = new PrintWriter(new File(outputName))
    writer.write(result.toString())
    writer.close()
  }

  val filePath = "u.data"
  val myFilmID: Int = 300
  val (myFilm, allFilms) = Read(filePath, myFilmID)
  Write(myFilm, allFilms, "hist_film", "hist_all", "lab01.json")
}

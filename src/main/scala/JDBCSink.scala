import java.sql._
import org.apache.spark.sql.{ForeachWriter, Row}

class  JDBCSink(url:String, user:String, pwd:String, driver:String) extends ForeachWriter[org.apache.spark.sql.Row] {
  var connection:Connection = _
  var statement:Statement = _

  def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: Row): Unit = {
    statement.executeUpdate("INSERT INTO zip_test (nama,jumlah) " +
      "VALUES ('" + value(0) + "'," + value(1) + ");")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}


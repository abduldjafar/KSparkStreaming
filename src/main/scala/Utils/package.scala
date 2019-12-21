import java.util.regex.Pattern
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

package object Utils {
  def regexp_extractAll: UserDefinedFunction = udf(f = (job: String, exp: String, groupIdx: Int) => {
    val pattern = Pattern.compile(exp)
    val m = pattern.matcher(job)
    var result = Seq[String]()
    while (m.find) {
      val temp: Unit =
        result = result :+ m.group(groupIdx)
    }
    result.toArray
  })
}

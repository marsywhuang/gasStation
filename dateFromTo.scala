import java.util.Calendar
import java.text.SimpleDateFormat

val dateFromat = new SimpleDateFormat("yyyy-MM-dd")

val dateFrom = Calendar.getInstance()
val dateTo = Calendar.getInstance()

// 起始日期
dateFrom.set(Calendar.YEAR, 2016)
dateFrom.set(Calendar.MONTH, 0)
dateFrom.set(Calendar.DATE, 1)

dateFromat.format(dateFrom.getTime)

// 起迄日期
dateTo.set(Calendar.YEAR, 2016)
dateTo.set(Calendar.MONTH, 11)
dateTo.set(Calendar.DATE, 31)

dateFromat.format(dateTo.getTime)

// 列出所有日期
import java.util.Calendar
import org.joda.time.{DateTime, LocalDate, DurationFieldType}

def allDaysForYear(year: String): List[(String, String, String)] = {
  val dateTime = new DateTime()
  val daysInYear = if(dateTime.withYear(year.toInt).year.isLeap) 366 else 365
  val calendar = Calendar.getInstance
  //
  calendar.set(year.toInt, 0, 0)
  //
  val ld = LocalDate.fromCalendarFields(calendar)
  for {
    day <- (1 to daysInYear).toList
    updatedLd = ld.withFieldAdded(DurationFieldType.days, day)
  } yield (year, updatedLd.getMonthOfYear.toString, updatedLd.getDayOfMonth.toString)
}

for (s <- datePeriod.toList) {
  print(s)
}

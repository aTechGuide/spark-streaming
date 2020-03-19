package guide.atech.schema

import java.sql.Date

case class Stock(
                  company: String,
                  date: Date,
                  value: Double
                )

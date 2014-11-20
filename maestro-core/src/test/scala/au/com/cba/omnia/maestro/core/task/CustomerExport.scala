package au.com.cba.omnia.maestro.core.task

import scalikejdbc.{SQL, ConnectionPool, AutoSession}

object CustomerExport {
  Class.forName("org.hsqldb.jdbcDriver")

  def tableSetup(
    connectionString: String,
    username: String,
    password: String,
    table: String = "customer_export",
    data: Option[List[String]] = None
  ): Unit = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    SQL(s"""
      create table $table (
        id integer,
        name varchar(20),
        accr varchar(20),
        cat varchar(20),
        sub_cat varchar(20),
        balance integer
      )
    """).execute.apply()

    data.foreach(lines =>
      lines.foreach(line => {
        val row = line.split('|')
        SQL(s"""insert into ${table}(id, name, accr, cat, sub_cat, balance)
          values ('${row(0)}', '${row(1)}', '${row(2)}', '${row(3)}', '${row(4)}', '${row(5)}')"""
        ).update().apply()
      })
    )
  }

  def tableData(
    connectionString: String,
    username: String,
    password: String,
    table: String = "customer_export"
  ): List[String] = {
    ConnectionPool.singleton(connectionString, username, password)
    implicit val session = AutoSession
    SQL(s"select * from $table").map(rs => List(rs.int("id"), rs.string("name"), rs.string("accr"),
      rs.string("cat"), rs.string("sub_cat"), rs.int("balance")) mkString "|").list.apply()
  }
}

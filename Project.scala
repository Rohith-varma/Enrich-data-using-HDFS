package ca.rohith.bigdata.hadoop

import scala.io.Source
import org.apache.hadoop.fs.{FSDataInputStream,Path,FSDataOutputStream}

case class Trip(route_id: Int,
                service_id: String,
                trip_id: String,
                trip_headsign: String,
                direction_id: Int,
                shape_id: Int,
                wheelchair_accessible: Int,
                note_fr: String,
                note_en: String)

case class Route(route_id: Int,
                 agency_id: String,
                 route_short_name: Int,
                 route_long_name: String,
                 route_type: Int,
                 route_url: String,
                 route_color: String,
                 route_text_color: Any)

case class Calender(service_id: String,
                    monday: Int,
                    tuesday: Int,
                    wednesday: Int,
                    thursday: Int,
                    friday: Int,
                    saturday: Int,
                    sunday: Int,
                    start_date: String,
                    end_date: String)

case class TripRoute(Service_id: String, route: Route, trip: Trip)

case class EnrichedTrip(tripRoute: TripRoute, calender: Calender)

case class RouteLookup(routes: List[Route]) {
  private val lookupTable: Map[Int, Route] =
    routes.map(route => route.route_id -> route).toMap

  def lookup(routeId: Int): Route = lookupTable.getOrElse(routeId, null)
}

case class CalendarLookup(calendars: List[Calender]) {
  private val lookupTable: Map[String, Calender] =
    calendars.map(calendar => calendar.service_id -> calendar).toMap

  def lookup(serviceId: String): Calender = lookupTable.getOrElse(serviceId, null)
}

object Project extends HdfsClient {

  def applyRoute(csvline: String): Route = {
    val a: List[String] = csvline.split(",", -1).toList
    Route(a(0).toInt, a(1), a(2).toInt, a(3), (a(4).toInt), a(5), a(6), a(7))
  }

  def applyTrip(csvline: String): Trip = {
    val a: List[String] = csvline.split(",", -1).toList
    Trip(a(0).toInt, a(1), a(2), a(3), (a(4).toInt), a(5).toInt, a(6).toInt, a(7), a(8))
  }

  def applyCalendar(csvline: String): Calender = {
    val a: List[String] = csvline.split(",").toList
    Calender(a(0), a(1).toInt, a(2).toInt, a(3).toInt, (a(4).toInt), a(5).toInt, a(6).toInt, a(7).toInt, a(8), a(9))
  }

  val fileSource: FSDataInputStream = fs.open(new Path("/user/winter2020/rohith/stm/routes.txt"))
  val routeFileSource = Source.fromInputStream(fileSource).getLines.toList
  val route = routeFileSource.map(applyRoute)
  val routes = RouteLookup(route)

  val fileSource1: FSDataInputStream = fs.open(new Path("/user/winter2020/rohith/stm/trips.txt"))
  val tripFileSource = Source.fromInputStream(fileSource1).getLines.toList
  val trips = tripFileSource.map(applyTrip)

  val fileSource2: FSDataInputStream = fs.open(new Path("/user/winter2020/rohith/stm/calendar.txt"))
  val calenderFileSource = Source.fromInputStream(fileSource2).getLines.toList
  val calendar = calenderFileSource.map(applyCalendar)
  val calendars = CalendarLookup(calendar)

  val tripRouteData = trips.map(trip => {
    val route: Route = routes.lookup(trip.route_id)
    val Service_id: String = trip.service_id
    TripRoute(Service_id, route, trip)
  })

  val enrichTrip = tripRouteData.map(triproute => {
    val Service_id: String = triproute.Service_id
    val calender = calendars.lookup(Service_id)
    EnrichedTrip(triproute, calender)
  })

  val outputFile = new Path("/user/winter2020/rohith/course3/EnrichedTrip.csv")
  val output: FSDataOutputStream = fs.create(outputFile)

  output.writeChars("service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date," +
    "route_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en,agency_id," +
    "route_short_name,route_long_name,route_type,route_url,route_color,route_text_color\n")

  for {i <- enrichTrip
       j <- List(i.tripRoute)} {
    val k = calendars.lookup(j.Service_id)
    val temp = (k.service_id + "," + k.monday + "," + k.tuesday + "," + k.wednesday + "," + k.thursday + ","
      + k.friday + "," + k.saturday + "," + k.sunday + "," + k.start_date + "," + k.end_date + ",")
    output.writeChars(temp)
    for (k <- List(j.trip)) {
      output.writeChars(k.route_id + "," + k.trip_id + "," + k.trip_headsign + "," + k.direction_id + ","
        + k.shape_id + "," + k.wheelchair_accessible + "," + k.note_fr + "," + k.note_en + ",")
    }
    for (k <- List(j.route)) {
      output.writeChars(k.agency_id + "," + k.route_short_name + "," + k.route_long_name + ","
        + k.route_type + "," + k.route_url + "," + k.route_color + "," + k.route_text_color)
    }
  }
  output.writeChars("\n")
  output.close()
}





import java.time.LocalDateTime

case class Trip(
                 tripId:Integer, //id
                 duration:Integer, //duration
                 startDate:LocalDateTime, //start_date
                 startStation:String, //start_station_name
                 startTerminal:Integer, //start_station_id
                 endDate:LocalDateTime, //end_date
                 endStation:String, //end_station_name
                 endTerminal:Integer, //end_station_id
                 bikeId: Integer, //bike_id
                 subscriptionType: String, //subscription_type
                 zipCode: String //zip_code
               ) extends Ordered[Trip]{
  override def compare(that: Trip): Int = this.startDate.compareTo(that.startDate)
}
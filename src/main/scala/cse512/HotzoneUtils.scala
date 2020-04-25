package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean =
  {
    // YOU NEED TO CHANGE THIS PART

    if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty())
      return false

  	val rectangleCoordinates = queryRectangle.split(",")
  	var x1 = rectangleCoordinates(0).toDouble
  	var y1 = rectangleCoordinates(1).toDouble
  	var x2 = rectangleCoordinates(2).toDouble
  	var y2 = rectangleCoordinates(3).toDouble

  	val pointCoordinates = pointString.split(",")
  	var x = pointCoordinates(0).toDouble
  	var y = pointCoordinates(1).toDouble

  	if (x >= x1 && x <= x2 && y >= y1 && y <= y2)
  	    return true
  	else if (x >= x2 && x <= x1 && y >= y2 && y <= y1)
  	    return true
    else
        return false
   // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}

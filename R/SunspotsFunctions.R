# ------------------------------------------------------------------------------
# Functions for fetching sunspots data from the SILSO website:
# http://sidc.be/silso/home
# ------------------------------------------------------------------------------


#' Fetch the SILSO sunspots time series
#'
#' Fetch full raw daily SILSO sunspots time series
#'
#' This function fetches the sunspots data published by SILSO:
#' @return data.table
#' @export
dtDownloadAllRawSunspotsTimeSeries <- function() {

  # 1. definitions of the columns: archived ------------------------------------
  cArchivedColNames <- c("date_year", "date_month", "date_day", "date_yearfraction",
                         "sunspot_number", "sunspot_number_std",
                         "number_of_observations_used", "is_value_definitive")
  cArchivedColTypes <- c("integer", "integer", "integer", "double",
                         "double", "double",
                         "integer", "integer")

  # 2. fetching the core dataset -----------------------------------------------
  dtCoreSunspots <- tryCatch(expr = {
    message("Fetching the archived data on the sunspots number... ")
    data.table::fread(input = "http://sidc.be/silso/INFO/sndtotcsv.php",
                      fill = TRUE, sep = ";", stringsAsFactors = FALSE,
                      header = FALSE, col.names = cArchivedColNames,
                      colClasses = cArchivedColTypes)
  }, error = function(er) {
    message("Error occured when trying to fetch the data from http://sidc.be/silso/INFO/sndtotcsv.php. ",
            "Returning NULL! ")
    return(NULL)
  }, finally = {
    message("After fetching the archived data on the sunspots number! ")
  })

  # 4. definitions of the columns: current month -------------------------------
  cCurrentMonthColNames <- c("date_year", "date_month", "date_day", "date_yearfraction",
                             "sunspot_number", "sunspot_number_std",
                             "number_of_observations_used", "number_of_all_observations",
                             "is_redundant")
  cCurrentMonthColTypes <- c("integer", "integer", "integer", "double",
                             "double", "double", "integer", "integer",
                             "logical")

  # 5. fetching the current month's data ---------------------------------------
  dtCurrentMonthSunspots <- tryCatch(expr = {
    message()
    read.table(file = "http://sidc.be/silso/DATA/EISN/EISN_current.csv", sep = ",",
               header = FALSE, dec = ".", stringsAsFactors = FALSE,
               col.names = cColNames, colClasses = cColTypes) %>%
      data.table::as.data.table()
  }, error = function(er) {
    message()
    return(NULL)
  }, finally = {
    message()
  })

  # 6. check if the data fetching has been successful and ----------------------
  #    concatenate the data
  number_of_all_observations <- is_redundant <- NULL
  dtCurrentMonthSunspots[, redundant := NULL]
  dtCurrentMonthSunspots[, number_of_all_observations := NULL]
  dtCurrentMonthSunspots[, is_value_definitive := 1L]
  dtJoinedData <- data.table::rbindlist(
    l = list(dtCoreSunspots, dtCurrentMonthSunspots),
    use.names = TRUE, fill = TRUE)

  return(dtJoinedData)
}




#'
#'
#'


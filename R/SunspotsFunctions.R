# ------------------------------------------------------------------------------
# Functions for fetching sunspots data from the SILSO website:
# http://sidc.be/silso/home
# ------------------------------------------------------------------------------

#' Fetch the archived SILSO sunspots daily time series
#'
#' Fetch full raw archived daily SILSO sunspots time series
#'
#' This function fetches the archived time series of sunspots published by SILSO at
#' http://sidc.be/silso/INFO/sndtotcsv.php
#' It is stressed that the returned data is raw (e.g. the rows with missing
#' observations are not excluded, etc.)
#' @return data.table in case the fetch is successful; NULL otherwise; The returned
#' data.table has following columns whose names are largely self-explanatory:
#' 1) "date_year", 2) "date_month", 3) "date_day", 4) "date_yearfraction",
#' 5) "sunspot_number", 6) "sunspot_number_std", 7) "number_of_observations_used",
#' 8) "is_value_definitive"
#' @export
dtDownloadArchivedSilsoSunspotsTimeSeries <- function() {

  # 1. definitions of the columns: archived ------------------------------------
  cArchivedColNames <- c("date_year", "date_month", "date_day", "date_yearfraction",
                         "sunspot_number", "sunspot_number_std",
                         "number_of_observations_used", "is_value_definitive")
  cArchivedColTypes <- c("integer", "integer", "integer", "double",
                         "double", "double",
                         "integer", "integer")

  # 2. fetching the core dataset -----------------------------------------------
  dtCoreSunspots <- tryCatch(expr = {
    message("Fetching the archived SILSO data on the sunspots number... ")
    data.table::fread(input = "http://sidc.be/silso/INFO/sndtotcsv.php",
                      fill = TRUE, sep = ";", stringsAsFactors = FALSE,
                      header = FALSE, col.names = cArchivedColNames,
                      colClasses = cArchivedColTypes)
  }, error = function(er) {
    message("Error occured when trying to fetch the data from http://sidc.be/silso/INFO/sndtotcsv.php. ",
            "Returning NULL! Full error message: ", er)
    return(NULL)
  }, finally = {
    message("After fetching the archived SILSO data on the sunspots number. ")
  })
  if (data.table::is.data.table(x = dtCoreSunspots)) {
    message("Successfully fetched the archived SILSO sunspots data!")
  }

  return(dtCoreSunspots)
}



#' Fetch the SILSO sunspots daily time series for current month
#'
#' Fetch the daily SILSO sunspots time series for the current month (updated daily)
#'
#' This function fetches the time series of sunspots published by SILSO for the
#' current month. The specific file from which the data is fetched is:
#'
#' It is stressed that the returned data is raw (e.g. the rows with missing
#' observations are not excluded, etc.)
#' @return data.table in case the fetch is successful; NULL otherwise; The returned
#' data.table has following columns whose names are largely self-explanatory:
#' 1) "date_year", 2) "date_month", 3) "date_day", 4) "date_yearfraction",
#' 5) "sunspot_number", 6) "sunspot_number_std", 7) "number_of_observations_used",
#' 8) "is_value_definitive"
#' @export
dtDownloadCurrentMonthSilsoSunspotsTimeSeries <- function() {

  # 1. definitions of the columns: current month -------------------------------
  cCurrentMonthColNames <- c("date_year", "date_month", "date_day", "date_yearfraction",
                             "sunspot_number", "sunspot_number_std",
                             "number_of_observations_used", "number_of_all_observations",
                             "redundant_col")
  cCurrentMonthColTypes <- c("integer", "integer", "integer", "double",
                             "double", "double", "integer", "integer",
                             "logical")

  # 2. fetching the current month's data ---------------------------------------
  dtCurrentMonthSunspots <- tryCatch(expr = {
    message("Fetching the SILSO data on the sunspots number for the current month... ")
    read.table(file = "http://sidc.be/silso/DATA/EISN/EISN_current.csv", sep = ",",
               header = FALSE, dec = ".", stringsAsFactors = FALSE,
               col.names = cCurrentMonthColNames,
               colClasses = cCurrentMonthColTypes) %>%
      data.table::as.data.table()
  }, error = function(er) {
    message("Error occured when trying to fetch the data from http://sidc.be/silso/DATA/EISN/EISN_current.csv; ",
            "Returning NULL! Full error message: ", er)
    return(NULL)
  }, finally = {
    message("After fetching the SILSO data on the sunspots number for the current month. ")
  })
  if (data.table::is.data.table(x = dtCurrentMonthSunspots)) {
    message("Successfully fetched the SILSO sunspots data for the current month!")
    is_value_definitive <- number_of_all_observations <- redundant_col <- NULL
    dtCurrentMonthSunspots[, redundant_col := NULL]
    dtCurrentMonthSunspots[, number_of_all_observations := NULL]
    dtCurrentMonthSunspots[, is_value_definitive := 0L]
  }

  return(dtCurrentMonthSunspots)
}



#' Fetch the SILSO sunspots time series
#'
#' Fetch full raw daily SILSO sunspots time series
#'
#' This function fetches the sunspots data published by SILSO at
#' http://sidc.be/silso/home and prepares the full time series of all the
#' observations that have been made available, including for the current month.
#' @return data.table with the following columns:
#' 1) "date_year", 2) "date_month", 3) "date_day", 4) "date_yearfraction",
#' 5) "sunspot_number", 6) "sunspot_number_std", 7) "number_of_observations_used",
#' 8) "is_value_definitive"
#' @export
dtDownloadCompleteSilsoSunspotsTimeSeries <- function() {

  # 1. fetch the archived data -------------------------------------------------
  dtArchived <- dtDownloadArchivedSilsoSunspotsTimeSeries()

  # 2. fetch the current month's data ------------------------------------------
  dtCurrentMonth <- dtDownloadCurrentMonthSilsoSunspotsTimeSeries()

  # 3. if both datasets have been fetched properly, concatenate; ---------------
  #    otherwise: raise an error
  if (!data.table::is.data.table(x = dtArchived) |
      !data.table::is.data.table(x = dtCurrentMonth)) {
    stop("Error occured inside dtDownloadCompleteSilsoSunspotsTimeSeries: either
         archived data or current month's data fetching has failed! ")
  } else {
    dtJoinedData <- data.table::rbindlist(
      l = list(dtArchived, dtCurrentMonth),
      use.names = TRUE, fill = TRUE)
  }

  return(dtJoinedData)
}




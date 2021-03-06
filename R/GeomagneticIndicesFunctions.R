# ------------------------------------------------------------------------------
# Functions for fetching data on geomagnetic indices from the GFZ
# German Research Centre for Geosciences at:
# https://www.gfz-potsdam.de/en/kp-index/
# ------------------------------------------------------------------------------


#' Get list of all Kp and Ap indices files
#'
#' Get list of all Kp and Ap indices files made available by GFZ Potsdam
#'
#' This function lists all the files that have been made available for FTP download
#' at the GFZ Potsdam's German Research Centre for Geosciences, cf.:
#' https://www.gfz-potsdam.de/en/kp-index/
#' Specific server location that is checked for files is:
#' ftp://ftp.gfz-potsdam.de/pub/home/obs/Kp_ap_Ap_SN_F107
#' @return character vector of all files available in the server location
#' @export
cGetAllAvailableFiles <- function() {

  # 1. prepare base of the URL
  cFtpBase <- "ftp://ftp.gfz-potsdam.de/pub/home/obs/Kp_ap_Ap_SN_F107/"

  # 2. make a handle
  objHandle <- curl::new_handle()

  # 3. make files fetching
  cFiles <- tryCatch(expr = {
    message("configuring the handle...")
    curl::handle_setopt(objHandle, ftp_use_epsv = TRUE, dirlistonly = TRUE)
    message("creating connection to ", cFtpBase)
    objConn <- curl::curl(url = cFtpBase, "r", handle = objHandle)
    message("loading data from ", cFtpBase)
    cFiles <- readLines(con = objConn, warn = TRUE)
    message("closing the connection with ", cFtpBase)
    close(objConn)
    cFiles
  }, error = function(er) {
    message("Error occurred when fetching list of files at ftp://ftp.gfz-potsdam.de/pub/home/obs/Kp_ap_Ap_SN_F107/!",
            "Returning NULL! The error message was: ", er)
    return(NULL)
  }, finally = {
    message("After fetching data from ", cFtpBase)
  })
  if (is.character(cFiles)) {
    message("Successfully fetched data from ", cFtpBase)
  }

  return(cFiles)
}



#' Download GFZ Potsdam geomagnetic indices observations
#'
#' Download the full time series of observations of geomagnetic indices Kp and ap
#' published by GFZ Potsdam
#'
#' This function downloads the time series of observations of the magnetic indices
#' Kp and ap performed by GFZ Potsdam and published by them. More information is
#' provided on the website https://www.gfz-potsdam.de/en/kp-index/
#' The files are fetched using the FTP protocol. This function downloads the
#' data from one of the files "Kp_ap_YYYY.txt"
#' @param iYear - integer scalar, not lower thant 1932 (scope of the data set).
#' This parameter indicates year for which the data is to be downloaded.
#' @return data.table with the downloaded data
#' @export
dtDownloadGeomagneticIndicesObsTimeSeriesForYear <- function(iYear) {

  # 1. input validation --------------------------------------------------------
  if (!bIsScalarOfType(objIn = iYear, cTypeName = "integer")) {
    stop("Error inside dtDownloadGeomagneticIndicesForYear: parameter ",
         "iYear is not an integer scalar! ")
  }
  if (iYear < 1932L) {
    stop("Error inside dtDownloadGeomagneticIndicesForYear: parameter ",
         "iYear cannot be lower than 1932 as there is no data available for the earlier years! ")
  }

  # 2. prepare file URL --------------------------------------------------------
  # 2.1. prepare base of the URL
  cFtpBase <- "ftp://ftp.gfz-potsdam.de/pub/home/obs/Kp_ap_Ap_SN_F107"
  # 2.2. make the name of the file
  cFileName <- paste0("Kp_ap_", iYear, ".txt")
  # 2.3. concatenate and get full URL
  cFullFileUrl <- file.path(cFtpBase, cFileName, fsep = "/")
  message("Accessing URL: ", cFullFileUrl)

  # 3. download the data -------------------------------------------------------
  # 3.1. prepare columns' configuration
  cColNames <- c("date_year", "date_month", "date_day",
                 "which_threehourly_interval", "specific_observation_time",
                 "days_since_origin_threehourly_interval",
                 "days_since_origin", "Kp", "ap", "is_definitive")
  cColClasses <- c("integer", "integer", "integer",
                   "character", "character",
                   "double",
                   "double", "double", "double", "integer")
  names(cColClasses) <- cColNames
  # 3.2. fetching the data with read.csv
  dtData <- tryCatch(expr = {
    # message("Calling read.csv on: ", cFullFileUrl)
    utils::read.csv(file = cFullFileUrl, header = FALSE, sep = "", dec = ".", skip = 30,
             col.names = cColNames, colClasses = cColClasses) %>%
      data.table::as.data.table()
  }, error = function(er) {
    stop("Error occurred when accessing the data at: ", cFullFileUrl,
         "; full error message: ", er)
  }, finally = {
    message("After data fetching.")
  })

  return(dtData)
}



#' Download GFZ Potsdam daily geomagnetic, flux and sunspots data
#'
#' Download the daily time series of observations of geomagnetic indices Kp and ap,
#' solar radio flux and sunspots published by GFZ Potsdam
#'
#' This function downloads the time series of observations of:
#' 1) magnetic indices Kp and ap
#' 2) international sunspot number
#' 3) solar radio flux
#' performed by GFZ Potsdam and published by them. More information is
#' provided on the website https://www.gfz-potsdam.de/en/kp-index/
#' The files are fetched using the FTP protocol.
#' This function downloads the data from one of the files "Kp_ap_SN_F107_YYYY.txt"
#' @param iYear - integer scalar, not lower thant 1932 (scope of the data set).
#' This parameter indicates year for which the data is to be downloaded.
#' @return data.table with the downloaded data
#' @export
dtDownloadGeomagneticIndicesWithSunspotsTimeSeriesForYear <- function(iYear) {

  # 1. input validation --------------------------------------------------------
  if (!bIsScalarOfType(objIn = iYear, cTypeName = "integer")) {
    stop("Error inside dtDownloadGeomagneticIndicesForYear: parameter ",
         "iYear is not an integer scalar! ")
  }
  if (iYear < 1932L) {
    stop("Error inside dtDownloadGeomagneticIndicesForYear: parameter ",
         "iYear cannot be lower than 1932 as there is no data available for the earlier years! ")
  }

  # 2. prepare file URL --------------------------------------------------------
  # 2.1. prepare base of the URL
  cFtpBase <- "ftp://ftp.gfz-potsdam.de/pub/home/obs/Kp_ap_Ap_SN_F107"
  # 2.2. make the name of the file
  cFileName <- paste0("Kp_ap_Ap_SN_F107_", iYear, ".txt")
  # 2.3. concatenate and get full URL
  cFullFileUrl <- file.path(cFtpBase, cFileName, fsep = "/")
  message("Accessing URL: ", cFullFileUrl)

  # 3. download the data -------------------------------------------------------
  # 3.1. prepare columns' configuration
  cColNames <- c("date_year", "date_month", "date_day",
                 "days_since_origin", "days_since_origin_threehourly_interval",
                 "Bartels_solar_rotation_number", "day_within_Bartels_solar_rotation_number",
                 paste0("Kp", 1:8), paste0("ap", 1:8),
                 "daily_mean_of_Ap", "international_sunspot_number",
                 "observed_F10.7_solar_flux", "adjusted_F10.7_solar_flux",
                 "is_definitive_indicator")
  cColClasses <- c("integer", "integer", "integer",
                   "integer", "double",
                   "integer", "integer",
                   rep("double", 8), rep("integer", 8),
                   "integer", "integer",
                   "double", "double",
                   "integer")
  names(cColClasses) <- cColNames
  # 3.2. fetching the data with read.csv
  dtData <- tryCatch(expr = {
    message("Calling read.csv on: ", cFullFileUrl)
    utils::read.csv(file = cFullFileUrl, header = FALSE, sep = "", dec = ".", skip = 40,
             col.names = cColNames, colClasses = cColClasses) %>%
      data.table::as.data.table()
  }, error = function(er) {
    stop("Error occurred when accessing the data at: ", cFullFileUrl,
         "; full error message: ", er)
  }, finally = {
    message("After data fetching.")
  })

  return(dtData)
}




# ------------------------------------------------------------------------------
# Functions for fetching data on geomagnetic indices from the GFZ
# German Research Centre for Geosciences at:
# https://www.gfz-potsdam.de/en/kp-index/
# ------------------------------------------------------------------------------


#'
#'
#'
cGetAllAvailableFiles <- function() {
  ftp_base <- "ftp://ftp.gfz-potsdam.de/pub/home/obs/Kp_ap_Ap_SN_F107/"
  list_files <- curl::new_handle()
  curl::handle_setopt(list_files, ftp_use_epsv = TRUE, dirlistonly = TRUE)
  con <- curl::curl(url = ftp_base, "r", handle = list_files)
  files <- readLines(con)
  close(con)
  return(files)
}



lLoadRawGeomagneticDataForYear <- function(iYear, bWithSunspots = FALSE) {

  # 1. input validation


  # 2. prepare file URL
  iYear <- 2000
  cFtpBase <- "ftp://ftp.gfz-potsdam.de/pub/home/obs/Kp_ap_Ap_SN_F107/"

  if (bWithSunspots = FALSE) {
    cFileUrl <- paste0(cFtpBase, "Kp_ap_", iYear, ".txt")
  } else {

  }

  res <- read.table(file = cFileUrl,
                         header = FALSE,
                         stringsAsFactors = FALSE,
                         sep = "",
                         skip = 29)
  head(res)

}






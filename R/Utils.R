
bIsScalarOfClass <- function(objIn, cClassName) {
  return(methods::is(object = objIn, class2 = cClassName) & length(objIn) == 1L)
}


bIsScalarOfType <- function(objIn, cTypeName) {
  return(typeof(x = objIn) == cTypeName & length(objIn) == 1L)
}

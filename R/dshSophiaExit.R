#' Log out from the SOPHIA federated database 
#'
#' Wrapper around `DSI::datashield.logout` that logs the user out from the federated system. 
#' @return Nothing, the user is logged out. 
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect()
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # -- do some work --
#'
#' # log out
#' dshSophiaExit()
#' }
#' @import DSI
#' @export
dshSophiaExit <- function() {
    tryCatch(
        expr = { DSI::datashield.logout(opals) },
        error = function(e) { message("\nUnable to disconnect, are you sure you are connected?") }
    )

    cat("\n")
}

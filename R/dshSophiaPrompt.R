#' Prompt for log in details 
#'
#' Prompts the user for login details, if those are not available via `Sys.getenv()`, and then connects via `dshSophiaConnect`. During login, the user may supply a single character or a list of characters separated by a single space denoting the nodes to either include or exclude. The function is primarily a fallback used within `dshSophiaLoad when the user has not logged in to the federated system. 
#' @return An Opals object (`opals`) is assigned to the Global environment.
#' @import DSOpal opalr httr DSI progress R6 
#' @export
dshSophiaPrompt <- function() {

    # check if the user has environment files with username and password,
    # if not then prompt for username and password
    if (identical(grep("fdb_user", names(Sys.getenv())), integer(0)) == FALSE) {
        username <- Sys.getenv("fdb_user")
    } else {
        username <- readline("Username: " )
    }

    if (identical(grep("fdb_password", names(Sys.getenv())), integer(0)) == FALSE) {
        password <- Sys.getenv("fdb_password")
    } else {
        password <- readline("Password: " )
    }

    # allow user to include/exclude nodes
    include <- strsplit(readline("Enter nodes to include, separated by space (press Enter to skip): "), " ")
    exclude <- strsplit(readline("Enter nodes to exclude, separated by space (press Enter to skip): "), " ")
    
    # readline returns a 'character(0)' even if there is no input;
    # these should be considered NULL values for dshSophiaConnect to work
    # (note that insterestingly, this is not possible to do using ifelse, see:
    # https://stackoverflow.com/a/61885348)
    if (identical(include[[1]], character(0)) == TRUE) include <- NULL else include <- include
    if (identical(exclude[[1]], character(0)) == TRUE) exclude <- NULL else exclude <- exclude

    dshSophiaConnect(username = username,
                     password = password,
                     include = include,
                     exclude = exclude)
}

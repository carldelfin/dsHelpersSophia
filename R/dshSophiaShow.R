#' Show available nodes and cohorts 
#'
#' Downloads an up-to-date .csv file from SIB containing node status and URL, then
#' via a two-step process logs in to each node to retrieve cohort names. The output
#' is a dataframe containing all available nodes and cohorts.
#' @param username A character, your username. Defaults to `Sys.getenv("fdb_user")`.
#' @param password A character, your password. Defaults to `Sys.getenv("fdb_password")`.
#' @return A data frame with all available nodes and cohorts in 'long' format, along with date if creation and last update.
#' @examples
#' \dontrun{
#' # show all nodes and cohorts, assuming username and password is specified in .Renviron:
#' dshSophiaShow()
#'
#' # show all nodes and cohorts, manually providing username and password:
#' dshSophiaShow(username = "user1", password = "pass1")
#' }
#' @import DSOpal opalr httr DSI
#' @importFrom utils download.file read.csv
#' @export
dshSophiaShow <- function(username = Sys.getenv("fdb_user"),
                          password = Sys.getenv("fdb_password"),
                          error = "exclude") {
    
    available_nodes <- tempfile() 
    download.file("https://sophia-fdb.vital-it.ch/nodes/status.csv", available_nodes)
    available_nodes <- read.csv(available_nodes) 
    available_nodes$node_name <- trimws(available_nodes$X, whitespace = ".*\\/")
    colnames(available_nodes) <- c("url", "error_code", "node_name")
    
    if (error == "exclude") {
        error_nodes <- subset(available_nodes, error_code == 1)
        available_nodes <- subset(available_nodes, error_code == 0)
        
        # let user know which nodes were excluded, if any
        if (!is.null(error_nodes)) {
            cat("\nNOTE! The following node(s) will be exluded due to connection errors:\n")
            cat(error_nodes$node_name, "\n")
        }
    }

    builder <- DSI::newDSLoginBuilder()
    for (i in 1:nrow(available_nodes)) {
        builder$append(server = available_nodes[i, "node_name"],
                       url = available_nodes[i, "url"],
                       user = username,
                       password = password,
                       driver = "OpalDriver")
    }

    # try to log in, and return opals object to global environment
    # NOTE:
    # this will overwrite any existing opals object
    tryCatch(
        expr = { opals <<- DSI::datashield.login(logins = builder$build()) },
        error = function(e) { message("\nUnable to connect, please check your credentials!") }
    )

    # get list of all available projects (cohorts)
    projects <- sapply(opals, function(x) opalr::opal.projects(x@opal),
                       simplify = FALSE)
    projects <- sapply(names(projects), 
        function(x) { return(projects[[x]][!(projects[[x]]$name %in% c("sophia", "omop_test", "a_test")), , drop = FALSE])
    }, simplify = FALSE)

    # create a data frame in long format with all cohorts (projects)
    # corresponding to each node; note that this is assigned to the 
    # Global environment for use by `dshSophiaLoad()`
    nodes_and_cohorts <<- do.call("rbind", projects)[, c(1, 4:5)]
    nodes_and_cohorts$node_name <- gsub("\\..*", "", rownames(nodes_and_cohorts))

    # return merged and cleaned results
    out <- merge(nodes_and_cohorts, available_nodes, by = "node_name", all = TRUE)[, c(1:4)]
    colnames(out) <- c("node", "cohort", "created", "last_updated")

    return(out)
}

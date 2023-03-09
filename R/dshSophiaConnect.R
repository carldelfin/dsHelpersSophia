#' Connect to SOPHIA nodes 
#'
#' Allows the user to connect to the SOPHIA federated database, with the option of including and excluding specific nodes (a node in this context is a server that hosts a database). Note that connecting to the federated database is a two-step process: First, the user connects to each individual node in order to retrieve a list of all cohorts that are hosted on that specific node (cohorts in this context refer to a specific dataset associated with a study or research project). Then, the user is disconnected, and then reconnects to each individual cohort.
#' @param username A character; your username. Defaults to `Sys.getenv("fdb_user")`.
#' @param password A character; your password. Defaults to `Sys.getenv("fdb_password")`.
#' @param include A character or vector of characters containing only the nodes you want to connect to. 
#' @param error A character; either `exclude` (default) or `include` (optional). If `exclude`, any nodes with an error will not be connected to. The user will notified if this is the case. If `include`, a connection attempt will be made, but is unlikely to succedd.
#' @param exclude A character or vector of characters containing only the nodes you do not want to connect to.
#' @return An Opals object (`opals`) is assigned to the Global environment.
#' @examples
#' \dontrun{
#' # connect to all nodes, assuming username and password is specified in .Renviron:
#' dshSophiaConnect()
#'
#' # connect to all nodes, manually providing username and password:
#' dshSophiaConnect(username = "username", password = "password")
#'
#' # include specific nodes:
#' dshSophiaConnect(include = c("node1", "node2"))
#'
#' # exclude specific nodes:
#' dshSophiaConnect(exclude = c("node3", "node4"))
#' }
#' @import DSOpal opalr httr DSI
#' @importFrom utils download.file read.csv
#' @export
dshSophiaConnect <- function(username = Sys.getenv("fdb_user"),
                             password = Sys.getenv("fdb_password"),
                             include = NULL,
                             exclude = NULL,
                             append_name = NULL,
                             restore = NULL) {

    available_nodes <- tempfile() 
    download.file("https://sophia-fdb.vital-it.ch/nodes/status.csv", available_nodes, quiet = TRUE)
    available_nodes <- read.csv(available_nodes) 
    available_nodes$node_name <- trimws(available_nodes$X, whitespace = ".*\\/")
    colnames(available_nodes) <- c("url", "error_code", "node_name")
    
    error_nodes <- subset(available_nodes, error_code == 1)
    available_nodes <- subset(available_nodes, error_code == 0)
        
    # let user know which nodes were exluded, if any
    if (!is.null(error_nodes)) {
        cat("\nNOTE! The following node(s) will be exluded due to connection errors:\n")
        cat(error_nodes$node_name, "\n")
    }
    
    #
    if (!is.null(include)) {
        if (include %in% error_nodes$node_name) {
            cat("\n")
            stop("The node you included is not available! Aborting...\n")
        }
    }

    if (!is.null(include)) { 
        available_nodes <- subset(available_nodes, node_name %in% include)
    } else if (!is.null(exclude)) {
        available_nodes <- subset(available_nodes, !node_name %in% exclude)
    }
    
    builder <- DSI::newDSLoginBuilder()
    
    for (i in 1:nrow(available_nodes)) {
        builder$append(server = available_nodes[i, "node_name"],
                       url = available_nodes[i, "url"],
                       user = username,
                       password = password,
                       driver = "OpalDriver")
    }

    # log in and return opals object to global environment
    # NOTE:
    # this will overwrite any existing opals object
    tryCatch(
        expr = { opals <<- DSI::datashield.login(logins = builder$build()) },
        error = function(e) { message("\nUnable to log in, please check your credentials!") }
    )
    
    # get list of all available projects (cohorts)
    projects <- sapply(opals, function(x) opalr::opal.projects(x@opal), simplify = FALSE)
    projects <- sapply(names(projects), function(x) {
        return(projects[[x]][!(projects[[x]]$name %in% c("sophia", "omop_test", "a_test", "big_db_test")), , drop = FALSE])
    }, simplify = FALSE)

    # create a dataframe in long format with all cohorts (projects)
    # corresponding to each node; note that this is assigned to the 
    # Global environment for use by `dshSophiaLoad()`
    nodes_and_cohorts <- do.call("rbind", projects)[, c(1, 4:5)]
    nodes_and_cohorts$node_name <- gsub("\\..*", "", rownames(nodes_and_cohorts))
    nodes_and_cohorts <<- nodes_and_cohorts

    # log out and create a new builder, so that we can connect to each cohort separately
    DSI::datashield.logout(opals)
    builder <- DSI::newDSLoginBuilder()
    
    for (i in 1:nrow(nodes_and_cohorts)) {
        tmp <- nodes_and_cohorts[i, ]
        server_short <- tmp$node_name
        
        if (is.null(append_name)) {
            server_name <- paste0(tmp$node_name, "_", tmp$name)
        } else {
            server_name <- paste0(tmp$node_name, "_", tmp$name, "_", append_name)
        }
        
        builder$append(server = server_name,
                       url = paste0("https://sophia-fdb.vital-it.ch/", server_short),
                       user = username,
                       password = password,
                       driver = "OpalDriver")

        rm(tmp, server_short, server_name)
    }

    if (is.null(restore)) {
        tryCatch(
                 expr = { opals <<- DSI::datashield.login(logins = builder$build()) },
                 error = function(e) { message("\nUnable to log in, please check your credentials!") }
        )
    } else {
        tryCatch(
                 expr = { opals <<- DSI::datashield.login(logins = builder$build(),
                                                          restore = restore) },
                 error = function(e) { message("\nUnable to log in, please check your credentials!") }
        )
    }

    # finally, let the user know which cohorts are accessible
    cat("\n\n")
    cat("You are now connected to the following", nrow(nodes_and_cohorts), "cohort(s) via the federated database:\n")
    cat(nodes_and_cohorts$name, "\n\n")
}

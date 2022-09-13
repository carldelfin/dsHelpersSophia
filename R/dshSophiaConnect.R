#' Connect to SOPHIA nodes 
#'
#' Allows the user to connect to the federated SOPHIA nodes, with the option of including and excluding specific nodes. Note that there are two rounds of connecting: First the user connects to each node in order to retrieve a list of all cohorts included at each node. Then, the user connects to each cohort separately. 
#' @param username A character; your username. Defaults to `Sys.getenv("fdb_user")`.
#' @param password A character; your password. Defaults to `Sys.getenv("fdb_password")`.
#' @param include A character or vector of characters containing only the nodes you want to connect to. 
#' @param exclude A character or vector of characters containing only the nodes you do not want to connect to.
#' @return An Opals object (`opals`) is assigned to the Global environment.
#' @examples
#' \dontrun{
#' # connect to all nodes, assuming username and password is specified in .Renvironment:
#' dshSophiaConnect()
#'
#' # connect to all nodes, manually providing username and password:
#' dshSophiaConnect(username = "user1", password = "pass1")
#'
#' # include specific nodes:
#' dshSophiaConnect(include = c("node1", "node2"))
#'
#' # exclude specific nodes:
#' dshSophiaConnect(exclude = c("node3", "node4"))
#' }
#' @export
#' @importFrom utils download.file read.csv
dshSophiaConnect <- function(username = Sys.getenv("fdb_user"),
                             password = Sys.getenv("fdb_password"),
                             include = NULL,
                             exclude = NULL) {

    available_nodes <- tempfile() 
    download.file("https://sophia-fdb.vital-it.ch/nodes/status.csv", available_nodes)
    available_nodes <- read.csv(available_nodes) %>%
        dplyr::rename(url = X, error_code = errcodes) %>%
        dplyr::mutate(node_name = trimws(url, whitespace = ".*\\/")) %>%
        dplyr::select(node_name, url, error_code)

    if (!is.null(include)) { 
        available_nodes <- available_nodes %>% dplyr::filter(node_name %in% include) 
    } else if (!is.null(exclude)) {
        available_nodes <- available_nodes %>% dplyr::filter(!node_name %in% exclude) 
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
    projects <- sapply(opals, function(x) opalr::opal.projects(x@opal),
                        simplify = FALSE)
    projects <- sapply(names(projects), 
                        function(x) {
                            return(projects[[x]][!(projects[[x]]$name %in% 
                                                   c("sophia", "omop_test", "a_test")), ,
                                   drop = FALSE])},
                        simplify = FALSE)

    # create a dataframe in long format with all cohorts (projects)
    # corresponding to each node
    nodes_and_cohorts <- dplyr::bind_rows(projects, .id = "node") 

    # log out and create a new builder, so that we can connect to each cohort separately
    DSI::datashield.logout(opals)
    builder <- DSI::newDSLoginBuilder()
    
    for (i in 1:nrow(nodes_and_cohorts)) {
        tmp <- nodes_and_cohorts[i, ]
        server_short <- tmp$node
        server_name <- paste0(tmp$node, "_", tmp$name)
        builder$append(server = server_name,
                       url = paste0("https://sophia-fdb.vital-it.ch/", server_short),
                       user = username,
                       password = password,
                       driver = "OpalDriver")

        rm(tmp, server_short, server_name)
    }

    tryCatch(
        expr = { opals <<- DSI::datashield.login(logins = builder$build()) },
        error = function(e) { message("\nUnable to log in, please check your credentials!") }
    )
}

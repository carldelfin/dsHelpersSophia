#' Show available nodes and cohorts 
#'
#' Downloads an up-to-date .csv file from SIB containing node status and URL, then
#' via a two-step process logs in to each node to retrive cohort names. The output
#' is a dataframe containing all available nodes and cohorts.
#' @param username A character, your username. Defaults to `Sys.getenv("fdb_user")`.
#' @param password A character, your password. Defaults to `Sys.getenv("fdb_password")`.
#' @return A dataframe with all available nodes and cohorts in 'long' format.
#' @examples
#' \dontrun{
#' # show all nodes and cohorts, assuming username and password is specified in .Renvironment:
#' dshSophiaShow()
#'
#' # show all nodes and cohorts, manually providing username and password:
#' dshSophiaShow(username = "user1", password = "pass1")
#' }
#' @export
#' @importFrom utils download.file read.csv
dshSophiaShow <- function(username = Sys.getenv("fdb_user"),
                          password = Sys.getenv("fdb_password")) {
    available_nodes <- tempfile() 
    download.file("https://sophia-fdb.vital-it.ch/nodes/status.csv", available_nodes)
    available_nodes <- read.csv(available_nodes) %>%
        dplyr::rename(url = X, error_code = errcodes) %>%
        dplyr::mutate(node = trimws(url, whitespace = ".*\\/")) %>%
        dplyr::select(node, url, error_code)
    
    builder <- DSI::newDSLoginBuilder()
    for (i in 1:nrow(available_nodes)) {
        builder$append(server = available_nodes[i, "node"],
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

    # return merged and cleaned results
    out <- merge(nodes_and_cohorts, available_nodes, by = "node", all = TRUE)  %>% 
        dplyr::rename(cohort = name) %>% 
        dplyr::select(node, cohort) %>%
        as.data.frame()

    return(out)
}

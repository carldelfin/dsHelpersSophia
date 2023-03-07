#' Load database resources 
#'
#' Loads all database resources on federated nodes. 
#' @return Nothing, all databases are loaded locally on each federated node.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect()
#'
#' # load database resources
#' dshSophiaLoad()
#' }
#' @import DSOpal opalr httr DSI
#' @importFrom utils menu 
#' @export
dshSophiaLoad <- function() {

    # if there is not an 'opals' or an 'nodes_and_cohorts' object in the Global environment,
    # the user probably did not run dshSophiaConnect() yet. Here the user may do so, after 
    # being promted for username and password.
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        cat("")
        cat("No 'opals' and/or 'nodes_and_cohorts' object found\n")
        cat("You probably did not run 'dshSophiaConnect' yet, do you wish to do that now?\n")
        switch(menu(c("Yes", "No (abort)")) + 1,
               cat("Test"), 
               dshSophiaPrompt(),
               stop("Aborting..."))
    }

    # now connect to each cohort individually
    for (i in 1:nrow(nodes_and_cohorts)) {
        
        skip_to_next <- FALSE
        
        tryCatch(
            expr = {
                tmp <- nodes_and_cohorts[i, ]
                this_opal <- datashield.connections_find()[[i]]@name
                this_project <- tmp$name
                res <- opalr::opal.resources(opals[[this_opal]]@opal, this_project)
                qualified_res_name <- paste0(this_project, ".", res$name)
                symbol_name <- paste0(this_project, "_", res$name)
                DSI::datashield.assign.resource(opals[this_opal], symbol_name, qualified_res_name)
            }, 
                
            error = function(e) { skip_to_next <<- TRUE }
        )
        
        if (skip_to_next) { next }
    }

    cat("\n")
}

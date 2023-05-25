#' Load SOPHIA federated database resources 
#'
#' Loads all database resources on federated nodes. 
#' @return Nothing, all databases are loaded locally on each federated node.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect(include = "abos")
#'
#' # load database resources
#' dshSophiaLoad()
#' }
#' @import DSOpal opalr httr DSI
#' @importFrom utils menu 
#' @export
dshSophiaLoad <- function() {
 
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        stop("\nNo 'opals' or 'nodes_and_cohorts' object found! Did you forget to run 'dshSophiaConnect'?")
    }
    
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

#' Get an overview of available data 
#'
#' Runs a query that iterates over every cohort and gathers information about the variables available in each of the cohort's OMOP tables.
#' @return A data frame.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect()
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # generate data overview
#' overview <- dshSophiaOverview()
#' }
#' @import DSOpal opalr httr DSI dsQueryLibrary rrapply purrr dplyr
#' @importFrom utils menu 
#' @export
dshSophiaOverview <- function() {

    # if there is not an 'opals' or an 'nodes_and_cohorts' object in the Global environment,
    # the user probably did not run dshSophiaConnect() yet. Here the user may do so, after 
    # being prompted for username and password.
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        cat("")
        cat("No 'opals' and/or 'nodes_and_cohorts' object found\n")
        cat("You probably did not run 'dshSophiaConnect' yet, do you wish to do that now?\n")
        switch(menu(c("Yes", "No (abort)")) + 1,
               cat("Test"), 
               dshSophiaPrompt(),
               stop("Aborting..."))
    }
    
    tryCatch(
        expr = { 
            # run query, which results in a nested list
            overview <- dsQueryLibrary::dsqRun(domain = "vocabulary",
                                               query_name = "V01",
                                               input = NULL,
                                               async = TRUE,
                                               datasources = opals)
           
            # flatten into list of data frames
            overview <- rrapply::rrapply(overview,
                                         is.data.frame,
                                         classes = "data.frame",
                                         how = "flatten")

            # map into single data frame by cohort
            overview <- purrr::map_df(overview,
                                      ~as.data.frame(.x),
                                      .id = "cohort")

            overview <- overview |>
                # remove redundant column
                dplyr::select(-tbl_concept_id) |>
                
                # remove '_db' after each cohort name
                dplyr::mutate(cohort = factor(gsub("(.*)_\\w+", "\\1", cohort))) |>

                # remove whitespace
                dplyr::mutate(table = gsub(" ", "", as.character(table)),
                              column_name = gsub(" ", "", as.character(column_name))) |>
                dplyr::mutate(table = factor(table),
                              column_name = factor(column_name)) |>

                # make 'concept_id' a numeric
                dplyr::mutate(concept_id = as.numeric(concept_id)) |>
                dplyr::select(cohort, dplyr::everything())

            return(overview)
        },
        error = function(e) { 
            message("\nUnable to load database resources, did you forget to run 'dshSophiaLoad()'?")
        }
    )
}

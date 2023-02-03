#' Get descriptive information about a variable in the measurement table
#'
#' Given a valid Concept ID the function gathers descriptive information about the corresponding variable in the measurement table and outputs a summary of the results. If the data is longitudinal the output will contain one row per time point.
#' @return A data frame.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect()
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # get descriptive information about BMI
#' df <- dshSophiaMeasureDesc(concept_id = 3038553)
#' }
#' @import DSOpal opalr httr DSI dsQueryLibrary dsBaseClient dsSwissKnifeClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaMeasureDesc <- function(variable, procedure_id = NA) {
    
    start <- Sys.time()

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
    
    # grouped by procedure?
    if (!is.na(procedure_id)) {
        
        # subset to procedure and variable
        dsSwissKnifeClient::dssSubset("baseline_tmp",
                                      "baseline",
                                      col.filter = paste0("c('", variable, "', 'has_", procedure_id, "')"),
                                      datasources = opals)
        
        # remove NAs (i.e., those without procedure _and_ those with missing measurements)
        dsBaseClient::ds.completeCases(x1 = "baseline_tmp",
                                       newobj = "baseline_tmp",
                                       datasources = opals)
        
    } else {
        
        # subset to variable and person_id
        dsSwissKnifeClient::dssSubset("baseline_tmp",
                                      "baseline",
                                      col.filter = paste0("c('person_id', '", variable, "')"),
                                      datasources = opals)
        
        # remove NAs
        dsBaseClient::ds.completeCases(x1 = "baseline_tmp",
                                       newobj = "baseline_tmp",
                                       datasources = opals)
    }
    
    concept_id <- stringr::str_split(variable, "_", n = 3)[[1]][[2]]
    tmp_summary <- dsBaseClient::ds.summary(paste0("baseline_tmp$", variable))
    
    if (length(tmp_summary[[1]]) == 1) {
        
        out <- data.frame(concept_id = concept_id,
                          time = NA,
                          type = NA,
                          n = NA,
                          mean = NA,
                          sd = NA,
                          se = NA,
                          median = NA,
                          q1 = NA,
                          q3 = NA,
                          iqr = NA,
                          min = NA,
                          max = NA)
        
    } else if (tmp_summary[[1]][[2]] < 5) {
        
        out <- data.frame(concept_id = concept_id,
                          time = NA,
                          type = NA,
                          n = NA,
                          mean = NA,
                          sd = NA,
                          se = NA,
                          median = NA,
                          q1 = NA,
                          q3 = NA,
                          iqr = NA,
                          min = NA,
                          max = NA)
        
    } else {
        
        tmp_summary <- tmp_summary[[1]]
        tmp_var <- dsBaseClient::ds.var(paste0("baseline_tmp$", variable))
        tmp_range <- dsSwissKnifeClient::dssRange(paste0("baseline_tmp$", variable))
        time <- stringr::str_split(variable, "_", n = 3)[[1]][[1]]
        
        if (length(stringr::str_split(variable, "_", n = 3)[[1]]) == 2) {
            
            type <- "raw_score"
            
        } else {
            
            type <- stringr::str_split(variable, "_", n = 3)[[1]][[3]]
            
        }
        
        out <- data.frame(concept_id = concept_id,
                          time = time,
                          type = type,
                          n = tmp_var[[1]][[3]],
                          mean = tmp_summary[[3]][[8]],
                          sd = sqrt(tmp_var[[1]][[1]]),
                          se = sqrt(tmp_var[[1]][[1]]) / sqrt(tmp_var[[1]][[4]]),
                          median = tmp_summary[[3]][[4]],
                          q1 = tmp_summary[[3]][[3]],
                          q3 = tmp_summary[[3]][[5]],
                          iqr = tmp_summary[[3]][[5]] - tmp_summary[[3]][[3]],
                          min = tmp_range[[1]][[1]][[1]],
                          max = tmp_range[[1]][[1]][[2]])
        
    }
    
    # add procedure
    if (!is.null(procedure_id)) {
        
        out$procedure <- procedure_id
        
    } else {
        
        out$procedure <- NA
        
    }
    
    end <- Sys.time()
    runtime <- round(as.numeric(difftime(end, start, units = "secs")), 0) 
    cat("\nDone with", variable, "after", runtime, "seconds\n")
    
    return(out)

}
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
dshSophiaMeasureDesc <- function(variable, keep_procedure = NA, keep_observation = NA, remove_procedure = NA, remove_observation = NA) {
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

    # remove procedure?
    if (!is.na(remove_procedure)) {
        p_fil <- paste0(", 'has_", remove_procedure, "'")
    } else {
        p_fil <- NULL
    }
    
    # keep procedure?
    if (!is.na(keep_procedure)) {
        p_fil <- paste0(", 'has_", keep_procedure, "'")
    } else {
        p_fil <- NULL
    }


    # remove observation?
    if (!is.na(remove_observation)) {
        o_fil <- paste0(", 'has_", remove_observation, "'")
    } else {
        o_fil <- NULL
    }
    
    # keep observation?
    if (!is.na(keep_observation)) {
        o_fil <- paste0(", 'has_", keep_observation, "'")
    } else {
        o_fil <- NULL
    }

    fil <- paste0("c('", variable, "'", p_fil, o_fil, ")")

    dsSwissKnifeClient::dssSubset("baseline_tmp",
                                  "baseline",
                                  col.filter = fil,
                                  datasources = opals)
 
    # remove procedure?
    if (!is.na(remove_procedure)) {
        dsSwissKnifeClient::dssSubset("baseline_tmp",
                                      "baseline_tmp",
                                      row.filter = paste0("has_", remove_procedure, " == 0"),
                                      datasources = opals)
    } 
    
    # keep procedure?
    if (!is.na(keep_procedure)) {
        dsSwissKnifeClient::dssSubset("baseline_tmp",
                                      "baseline_tmp",
                                      row.filter = paste0("has_", keep_procedure, " == 1"),
                                      datasources = opals)
    } 

    # remove observation?
    if (!is.na(remove_observation)) {
        dsSwissKnifeClient::dssSubset("baseline_tmp",
                                      "baseline_tmp",
                                      row.filter = paste0("has_", remove_observation, " == 0"),
                                      datasources = opals)
    }
    
    # remove observation?
    if (!is.na(keep_observation)) {
        dsSwissKnifeClient::dssSubset("baseline_tmp",
                                      "baseline_tmp",
                                      row.filter = paste0("has_", keep_observation, " == 1"),
                                      datasources = opals)
    }

    # remove NAs 
    dsBaseClient::ds.completeCases(x1 = "baseline_tmp",
                                   newobj = "baseline_tmp",
                                   datasources = opals)
    
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
     
    # remove observation
    if (!is.null(remove_observation)) {
        out$remove_observation <- remove_observation 
    } else {
        out$remove_observation <- NA
    }
    
    # keep observation
    if (!is.null(keep_observation)) {
        out$keep_observation <- keep_observation 
    } else {
        out$keep_observation <- NA
    }
    
    # remove procedure
    if (!is.null(remove_procedure)) {
        out$remove_procedure <- remove_procedure 
    } else {
        out$remove_procedure <- NA
    }
    
    # keep procedure
    if (!is.null(keep_procedure)) {
        out$keep_procedure <- keep_procedure 
    } else {
        out$keep_procedure <- NA
    }

    return(out)
}

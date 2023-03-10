#' Merge a (longitudinal) measurement table variable with the federated 'baseline' data frame
#'
#' Given a valid measurement table Concept ID, the function merges that variable (in wide format) with the 'baseline' data frame on the federated node. Note that the 'baseline' data frame must already exist. If the variable is available across several time points, all time points are included, and the raw difference plus percentage change from time point 1 to timepoint X is calculated.
#' @return Nothing, the federated 'baseline' data frame is appended.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect()
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # create a 'baseline' data frame on the federated node
#' dshSophiaCreateBaseline(concept_id = c(4111665, 3004410, 3001308))
#' 
#' # add a longitudinal measure
#' dshSophiaMergeLongMeas(concept_id = 3038553)
#' 
#' # check resul:qt
#' dsBaseClient::ds.summary("baseline")
#' }
#' @import DSOpal opalr httr DSI dsBaseClient dsSwissKnifeClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaGetBeta <- function(outcome, pred, covariate = NA, 
                             keep_procedure = NA, remove_procedure = NA,
                             keep_observation = NA, remove_observation = NA, 
                             standardize_all = FALSE, standardize_pred = TRUE) {
    
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        cat("")
        cat("No 'opals' and/or 'nodes_and_cohorts' object found\n")
        cat("You probably did not run 'dshSophiaConnect' yet, do you wish to do that now?\n")
        switch(menu(c("Yes", "No (abort)")) + 1,
               dshSophiaPrompt(),
               stop("Aborting..."))
    }

    # remove procedure?
    if (!is.na(remove_procedure)) {
        rp_fil <- paste0(", 'has_", remove_procedure, "'")
    } else {
        rp_fil <- NULL
    }
    
    # keep procedure?
    if (!is.na(keep_procedure)) {
        kp_fil <- paste0(", 'has_", keep_procedure, "'")
    } else {
        kp_fil <- NULL
    }


    # remove observation?
    if (!is.na(remove_observation)) {
        ro_fil <- paste0(", 'has_", remove_observation, "'")
    } else {
        ro_fil <- NULL
    }
    
    # keep observation?
    if (!is.na(keep_observation)) {
        ko_fil <- paste0(", 'has_", keep_observation, "'")
    } else {
        ko_fil <- NULL
    }
    
    # covariate(s)?
    if (!any(is.na(covariate))) {
        cov_fil <- paste0(", '", paste0(covariate, collapse = "', '"), "'")
        final_preds <- c(pred, covariate)
        covariate_names <- paste0(covariate, collapse = ".")
    } else {
        cov_fil <- NULL
        final_preds <- pred
        covariate_names <- "none" 
    }
                            
    pred_unit <- paste0("unit_", strsplit(as.character(pred), "_")[[1]][[2]])
    outcome_unit <- paste0("unit_", strsplit(as.character(outcome), "_")[[1]][[2]])
    
    fil <- paste0("c('person_id', ",
                  "'", outcome, "', ", 
                  "'", pred_unit, "', ", 
                  "'", outcome_unit, "', ", 
                  "'", pred, "'", 
                  cov_fil, rp_fil, ro_fil, kp_fil, ko_fil, ")")

    dsSwissKnifeClient::dssSubset("baseline_tmp",
                                  "baseline",
                                  col.filter = fil,
                                  datasources = opals)
    
    pred_unit <- ds.levels(paste0("baseline_tmp$", pred_unit))
    pred_unit <- gsub("unit.", "", pred_unit[[1]][[1]])
    
    outcome_unit <- ds.levels(paste0("baseline_tmp$", outcome_unit))
    outcome_unit <- gsub("unit.", "", outcome_unit[[1]][[1]])
 
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
    
    # keep observation?
    if (!is.na(keep_observation)) {
        dsSwissKnifeClient::dssSubset("baseline_tmp",
                                      "baseline_tmp",
                                      row.filter = paste0("has_", keep_observation, " == 1"),
                                      datasources = opals)
    }
    
    # remove NAs 
    invisible(dsBaseClient::ds.completeCases(x1 = "baseline_tmp",
                                             newobj = "baseline_tmp",
                                             datasources = opals))


    # scale all?
    if (standardize_all == TRUE) {
        dsSwissKnifeClient::dssScale("baseline_tmp",
                                     "baseline_tmp",
                                     datasources = opals)
    }

    # scale predictor(s)? 
    if (standardize_pred == TRUE) {

        scale_fil <- paste0("c('person_id', ",
                            "'", pred, "'", 
                            cov_fil, ")")

        dsSwissKnifeClient::dssSubset("baseline_pred",
                                      "baseline_tmp",
                                      col.filter = scale_fil,
                                      datasources = opals)
        
        dsSwissKnifeClient::dssScale("baseline_pred",
                                     "baseline_pred",
                                     datasources = opals)
        
        dsSwissKnifeClient::dssSubset("baseline_outcome",
                                      "baseline_tmp",
                                      col.filter = paste0("c('person_id', '", outcome, "')"),
                                      datasources = opals)
        
        dsSwissKnifeClient::dssJoin(c("baseline_outcome", "baseline_pred"),
                                    symbol = "baseline_tmp",
                                    by = "person_id",
                                    join.type = "full",
                                    datasources = opals)

        invisible(dsBaseClient::ds.rm(c("baseline_pred", "baseline_outcome")))

    }

    # need to create numeric gender if used as covariate
    if (!any(is.na(covariate))) {
        if ("gender" %in% covariate) {
            dsSwissKnifeClient::dssDeriveColumn("baseline_tmp",
                                                "gender",
                                                "as.numeric(gender) - 1",
                                                datasources = opals)
        }
    }

    # get a temporary summary
    tmp <- dsBaseClient::ds.summary(paste0("baseline_tmp$", pred))
    
    if (length(tmp[[1]]) == 1) {

        out <- data.frame(outcome = outcome,
                          outcome.unit = outcome_unit,
                          predictor = pred,
                          predictor.unit = pred_unit,
                          covariate = covariate_names,
                          valid.n = NA,
                          intercept.beta = NA,
                          intercept.se = NA,
                          intercept.p.val = NA,
                          intercept.ci.low = NA,
                          intercept.ci.high = NA,
                          predictor.beta = NA,
                          predictor.se = NA,
                          predictor.p.val = NA,
                          predictor.ci.low = NA,
                          predictor.ci.high = NA,
                          keep_procedure = keep_procedure,
                          remove_procedure = remove_procedure,
                          keep_observation = keep_observation,
                          remove_observation = remove_observation,
                          standardize_all = standardize_all,
                          standardize_pred = standardize_pred)

    } else {

        # numeric/integer outcome
        if (tmp[[1]][[1]] == "numeric" | tmp[[1]][[1]] == "integer") {

            # if outcome is NA, Inf, have mean == 0, or
            # length (valid N) < 5
            # return empty
            if (is.na(tmp[[1]][[3]][[8]]) | tmp[[1]][[3]][[8]] == 0 | tmp[[1]][[3]][[8]] == Inf | tmp[[1]][[2]] < 5) {

                # return empty 
                out <- data.frame(outcome = outcome,
                                  outcome.unit = outcome_unit,
                                  predictor = pred,
                                  predictor.unit = pred_unit,
                                  covariate = covariate_names,
                                  valid.n = "< 5",
                                  intercept.beta = NA,
                                  intercept.se = NA,
                                  intercept.p.val = NA,
                                  intercept.ci.low = NA,
                                  intercept.ci.high = NA,
                                  predictor.beta = NA,
                                  predictor.se = NA,
                                  predictor.p.val = NA,
                                  predictor.ci.low = NA,
                                  predictor.ci.high = NA,
                                  keep_procedure = keep_procedure,
                                  remove_procedure = remove_procedure,
                                  keep_observation = keep_observation,
                                  remove_observation = remove_observation,
                                  standardize_all = standardize_all,
                                  standardize_pred = standardize_pred)

            } else {

                tryCatch(expr = { 
                            
                             mod <- dsSwissKnifeClient::dssLM(what = "baseline_tmp",
                                                              type = "split",
                                                              dep_var = outcome,
                                                              expl_vars = final_preds,
                                                              datasources = opals) %>% 
                             as.data.frame() 

                         out <- data.frame(outcome = outcome,
                                           outcome.unit = outcome_unit,
                                           predictor = pred,
                                           predictor.unit = pred_unit,
                                           covariate = covariate_names,
                                           valid.n = tmp[[1]][[2]],
                                           intercept.beta = mod[1, 1],
                                           intercept.se = mod[1, 2],
                                           intercept.p.val = mod[1, 6],
                                           intercept.ci.low = mod[1, 3],
                                           intercept.ci.high = mod[1, 4],
                                           predictor.beta = mod[2, 1],
                                           predictor.se = mod[2, 2],
                                           predictor.p.val = mod[2, 6],
                                           predictor.ci.low = mod[2, 3],
                                           predictor.ci.high = mod[2, 4],
                                           keep_procedure = keep_procedure,
                                           remove_procedure = remove_procedure,
                                           keep_observation = keep_observation,
                                           remove_observation = remove_observation,
                                           standardize_all = standardize_all,
                                           standardize_pred = standardize_pred)

                                  },

                         error = function(e) {

                             message("Caught an error!\n\n")
                             print(e)
                             cat("\n\n")
                             print(datashield.errors())

                         })
            }
        } else {

            stop("No factors allowed as outcome!")     

        }

    }
    
    out$cohort <- strsplit(opals[[1]]@name, "_")[[1]][[1]]
    return(out)
}

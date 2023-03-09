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
    } else {
        cov_fil <- NULL
    }

    fil <- paste0("c('person_id', ",
                  "'", outcome, "', ", 
                  "'", pred, "'", 
                  cov_fil, rp_fil, ro_fil, kp_fil, ko_fil, ")")

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
    if ("gender" %in% covariate) {
        dsSwissKnifeClient::dssDeriveColumn("baseline_tmp",
                                            "gender",
                                            "as.numeric(gender) - 1",
                                            datasources = opals)
    }

    # get a temporary summary
    tmp <- dsBaseClient::ds.summary(paste0("baseline_tmp$", pred))
    
    if (length(tmp[[1]]) == 1) {

        out <- data.frame(outcome = outcome,
                          predictor = pred,
                          covariate = paste0(covariate, collapse = "."),
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
                                  predictor = pred,
                                  covariate = paste0(covariate, collapse = "."),
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
                                                              expl_vars = c(pred, covariate),
                                                              datasources = opals) %>% 
                             as.data.frame() %>% 
                             tibble::rownames_to_column("predictor") 

                         colnames(mod) <- gsub(paste0(names(opals), "."), "", colnames(mod))
                         res_intercept <- mod[1, ]
                         res_intercept$predictor <- "intercept"
                         res_predictor <- mod %>% filter(predictor == pred)

                         out <- data.frame(outcome = outcome,
                                           predictor = pred,
                                           covariate = paste0(covariate, collapse = "."),
                                           valid.n = tmp[[1]][[2]],
                                           intercept.beta = res_intercept$beta,
                                           intercept.se = res_intercept$SE,
                                           intercept.p.val = res_intercept$p.val,
                                           intercept.ci.low = res_intercept$CILow,
                                           intercept.ci.high = res_intercept$CIHigh,
                                           predictor.beta = res_predictor$beta,
                                           predictor.se = res_predictor$SE,
                                           predictor.p.val = res_predictor$p.val,
                                           predictor.ci.low = res_predictor$CILow,
                                           predictor.ci.high = res_predictor$CIHigh,
                                           keep_procedure = keep_procedure,
                                           remove_procedure = remove_procedure,
                                           keep_observation = keep_observation,
                                           remove_observation = remove_observation,
                                           standardize_all = standardize_all,
                                           standardize_pred = standardize_pred)

                                  },

                         error = function(e) {

                             message("Caught an error!")
                             print(e)
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

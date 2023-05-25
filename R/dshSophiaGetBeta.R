#' Estimate the linear association between two continuous variables
#'
#' Given the names of two numeric columns in a federated data frame, estimates their linear association and returns the beta value and associated metrics. Several checks are performed to make sure the function does not violate privacy rules. The user has the option to subset data by procedure and observation prior to estimating the association, as well as standardizing all data or only the outcome. The outcome may also be reversed, which is useful when negative values are considered 'good', such as percent weight loss. 
#' @param dataframe A character, the name of the federated data frame holding `outcome` and `predictor`. Defaults to `baseline`.
#' @param outcome A character, must correspond to a numerical column in the federated data frame.
#' @param predictor A character, must correspond to a numerical column in the federated data frame.
#' @param covariate A character or a vector of characters, corresponding column names to use as covariates. Defaults to `NA`.
#' @param keep_procedure A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will only keep rows with this procedure. Defaults to `NA`.
#' @param remove_procedure A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will remove all rows with this procedure. Defaults to `NA`.
#' @param keep_observation A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will only keep rows with this observation. Defaults to `NA`.
#' @param remove_observation A numeric, must be a valid Concept ID for `has_` column created using `dshSophiaCreateBaseline`. Will remove all rows with this observation. Defaults to `NA`.
#' @param standardize_all A Boolean, setting to `TRUE` will center and scale all numeric columns (`outcome`, `predictor`, any variables supplied to `covariate`). Defaults to `FALSE`.
#' @param standardize_predictor A Boolean, setting to `TRUE` will center and scale `predictor`. Defaults to `FALSE`.
#' @param reverse_outcome A Boolean, setting to `TRUE` will reverse the `outcome` column. Defaults to `FALSE`.
#' @return A data frame with the estimated linear association (beta value) and associated metrics. If privacy checks are not passed, a data frame with NAs will be returned.
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
#' @export
dshSophiaGetBeta <- function(dataframe = "baseline",
                             outcome, predictor, 
                             covariate = NA, 
                             keep_procedure = NA, remove_procedure = NA,
                             keep_observation = NA, remove_observation = NA, 
                             standardize_all = FALSE, standardize_predictor = TRUE,
                             reverse_outcome = FALSE) {
     
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        stop("\nNo 'opals' or 'nodes_and_cohorts' object found! Did you forget to run 'dshSophiaConnect'?")
    }
  
    # make sure no factors are supplied 
    if (dsBaseClient::ds.class(paste0(dataframe, "$", outcome))[[1]] == "factor") {
        stop("\nFactors are not allowed as outcome")
    }
    
    if (dsBaseClient::ds.class(paste0(dataframe, "$", predictor))[[1]] == "factor") {
        stop("\nFactors are not allowed as predictor")
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
        final_predictors <- c(predictor, covariate)
        covariate_names <- paste0(covariate, collapse = ".")
    } else {
        cov_fil <- NULL
        final_predictors <- predictor
        covariate_names <- "none" 
    }
                            
    fil <- paste0("c('person_id', ",
                  "'", outcome, "', ", 
                  "'", predictor, "'", 
                  cov_fil, rp_fil, ro_fil, kp_fil, ko_fil, ")")

    dsSwissKnifeClient::dssSubset("tmp",
                                  dataframe, 
                                  col.filter = fil,
                                  datasources = opals)
    
    # remove procedure?
    if (!is.na(remove_procedure)) {
        dsSwissKnifeClient::dssSubset("tmp",
                                      "tmp",
                                      row.filter = paste0("has_", remove_procedure, " == 0"),
                                      datasources = opals)
    } 
    
    # keep procedure?
    if (!is.na(keep_procedure)) {
        dsSwissKnifeClient::dssSubset("tmp",
                                      "tmp",
                                      row.filter = paste0("has_", keep_procedure, " == 1"),
                                      datasources = opals)
    } 

    # remove observation?
    if (!is.na(remove_observation)) {
        dsSwissKnifeClient::dssSubset("tmp",
                                      "tmp",
                                      row.filter = paste0("has_", remove_observation, " == 0"),
                                      datasources = opals)
    }
    
    # keep observation?
    if (!is.na(keep_observation)) {
        dsSwissKnifeClient::dssSubset("tmp",
                                      "tmp",
                                      row.filter = paste0("has_", keep_observation, " == 1"),
                                      datasources = opals)
    }
    
    # remove NAs 
    invisible(dsBaseClient::ds.completeCases(x1 = "tmp",
                                             newobj = "tmp",
                                             datasources = opals))
    
    
    # no need to keep going if nrow < 10 at this stage
    tmp_summary <- dsBaseClient::ds.summary("tmp")
    
    if (length(tmp_summary[[1]]) == 1) {
        
        out <- data.frame(outcome = outcome,
                          predictor = predictor,
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
                          standardize_predictor = standardize_predictor,
                          reverse_outcome = reverse_outcome)
        

    } else if (tmp_summary[[1]][[2]] < 10) {
         
        out <- data.frame(outcome = outcome,
                          predictor = predictor,
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
                          standardize_predictor = standardize_predictor,
                          reverse_outcome = reverse_outcome)
        
    } else {
        
        # scale all?
        if (standardize_all == TRUE) {
            dsSwissKnifeClient::dssScale("tmp",
                                         "tmp",
                                         datasources = opals)
        }
        
        # scale predictor(s)? 
        if (standardize_predictor == TRUE) {
            
            scale_fil <- paste0("c('person_id', ",
                                "'", predictor, "'", 
                                cov_fil, ")")
            
            dsSwissKnifeClient::dssSubset("baseline_predictor",
                                          "tmp",
                                          col.filter = scale_fil,
                                          datasources = opals)
            
            dsSwissKnifeClient::dssScale("baseline_predictor",
                                         "baseline_predictor",
                                         datasources = opals)
            
            dsSwissKnifeClient::dssSubset("baseline_outcome",
                                          "tmp",
                                          col.filter = paste0("c('person_id', '", outcome, "')"),
                                          datasources = opals)
            
            dsSwissKnifeClient::dssJoin(c("baseline_outcome", "baseline_predictor"),
                                        symbol = "tmp",
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            invisible(dsBaseClient::ds.rm(c("baseline_predictor", "baseline_outcome")))
            
        }
       
        # reverse outcome?
        if (reverse_outcome == TRUE) {
            dsSwissKnifeClient::dssDeriveColumn("tmp", 
                                                outcome, 
                                                paste0(outcome, " * -1"),
                                                datasources = opals) 
        }
        
        # need to create numeric gender if used as covariate
        if (!any(is.na(covariate))) {
            if ("gender" %in% covariate) {
                dsSwissKnifeClient::dssDeriveColumn("tmp",
                                                    "gender",
                                                    "as.numeric(gender) - 1",
                                                    datasources = opals)
            }
        }
        
        tmp <- dsBaseClient::ds.summary(paste0("tmp$", predictor))
        
        if (length(tmp[[1]]) == 1) {
            
            out <- data.frame(outcome = outcome,
                              predictor = predictor,
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
                              standardize_predictor = standardize_predictor,
                              reverse_outcome = reverse_outcome)
            
        } else {
            
            # numeric/integer outcome
            if (tmp[[1]][[1]] == "numeric" | tmp[[1]][[1]] == "integer") {
                
                # if outcome is NA, Inf, has mean == 0, or length (valid N) < 10, return empty
                if (is.na(tmp[[1]][[3]][[8]]) | tmp[[1]][[3]][[8]] == 0 | tmp[[1]][[3]][[8]] == Inf | tmp[[1]][[2]] < 10) {
                    
                    out <- data.frame(outcome = outcome,
                                      predictor = predictor,
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
                                      standardize_predictor = standardize_predictor,
                                      reverse_outcome = reverse_outcome)
                    
                } else {
                    
                    tryCatch(expr = { 
                        
                        mod <- dsSwissKnifeClient::dssLM(what = "tmp",
                                                         type = "split",
                                                         dep_var = outcome,
                                                         expl_vars = final_predictors,
                                                         datasources = opals) %>% 
                            as.data.frame() 
                        
                        out <- data.frame(outcome = outcome,
                                          predictor = predictor,
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
                                          standardize_predictor = standardize_predictor,
                                          reverse_outcome = reverse_outcome)
                        
                    },
                    
                    error = function(e) {
                        
                        print(e)
                        print(datashield.errors())
                        
                    })
                }
                
            } else {
                
                stop("No factors allowed as outcome!")     
                
            }
            
        }
    }
    
    out$cohort <- strsplit(opals[[1]]@name, "_")[[1]][[1]]
    return(out)
    
}

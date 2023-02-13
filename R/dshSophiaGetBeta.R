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
#' # check result
#' dsBaseClient::ds.summary("baseline")
#' }
#' @import DSOpal opalr httr DSI dsBaseClient dsSwissKnifeClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaGetBeta <- function(outcome, predictor, covariate = FALSE, subset_procedure = FALSE, standardized = TRUE) {
    
    # ----------------------------------------------------------------------------------------------
    # if there is not an 'opals' or an 'nodes_and_cohorts' object in the Global environment,
    # the user probably did not run dshSophiaConnect() yet. Here the user may do so, after 
    # being prompted for username and password.
    # ----------------------------------------------------------------------------------------------
    
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        cat("")
        cat("No 'opals' and/or 'nodes_and_cohorts' object found\n")
        cat("You probably did not run 'dshSophiaConnect' yet, do you wish to do that now?\n")
        switch(menu(c("Yes", "No (abort)")) + 1,
               cat("Test"), 
               dshSophiaPrompt(),
               stop("Aborting..."))
    }
  
    cat("\n\nModelling variable:", predictor, "\n\n")
    
    # create subset formula
    if (subset_procedure == FALSE) {
        
        if (covariate == FALSE) {
            
            cols <- paste0("colnames(baseline) %in% c('",
                           outcome,
                           "', '",
                           predictor,
                           "')")
            
        } else {
            
            cols <- paste0("colnames(baseline) %in% c('",
                           outcome,
                           "', '",
                           predictor,
                           "', '",
                           paste0(covariate, collapse = "', '"),
                           "')")
            
        }
        
        
    } else {
        
        if (covariate == FALSE) {
            
            cols <- paste0("colnames(baseline) %in% c('",
                           paste0("has_", subset_procedure),
                           "', '",
                           outcome,
                           "', '",
                           predictor,
                           "')")
            
        } else {
            
            cols <- paste0("colnames(baseline) %in% c('",
                           paste0("has_", subset_procedure),
                           "', '",
                           outcome,
                           "', '",
                           predictor,
                           "', '",
                           paste0(covariate, collapse = "', '"),
                           "')")
            
        }
        
    }
    
    # subset
    dsSwissKnifeClient::dssSubset("baseline_tmp", 
                                  "baseline",
                                  col.filter = cols,
                                  datasources = opals)
    
    if (!(subset_procedure == FALSE)) {
      
      # subset to procedure == 1 
      dsSwissKnifeClient::dssSubset("baseline_tmp",
                                    "baseline_tmp",
                                    row.filter = paste0("has_", subset_procedure, " == 1"))
    }
    
    # remove NAs
    dsBaseClient::ds.completeCases(x1 = "baseline_tmp",
                                   newobj = "baseline_tmp",
                                   datasources = opals)
    
    # get a temporary summary
    tmp <- dsBaseClient::ds.summary(paste0("baseline_tmp$", predictor))
    
    # numeric/integer outcome
    if (tmp[[1]][[1]] == "numeric" | tmp[[1]][[1]] == "integer") {
      
        # if outcome is NA, Inf, have mean == 0, or
        # length (valid N) < 20, or
        # tmp is an invalid object (too many NAs),
        # return empty
        if (is.na(tmp[[1]][[3]][[8]]) | tmp[[1]][[3]][[8]] == 0 | tmp[[1]][[3]][[8]] == Inf | tmp[[1]][[2]] < 20 | tmp[[1]] == "INVALID object!") {
            
            # get beta etc
            out <- data.frame(outcome = outcome,
                              predictor = predictor,
                              valid_n = NA,
                              beta = NA,
                              p.value = NA,
                              ci.low = NA,
                              ci.high = NA)
            
        } else {
            
            # scale if standardized output is requested (default is TRUE)
            if (standardized == TRUE) {
                dsSwissKnifeClient::dssScale("baseline_tmp",
                                             "baseline_tmp",
                                             datasources = opals)
                
            }
            
            if (covariate == FALSE) {
                
                formula <- as.formula(paste0(outcome, " ~ 1 +", predictor))
                
            } else {
                
                formula <- as.formula(paste0(outcome, " ~ 1 +", paste0(covariate, collapse = "+"), "+", predictor))
                
            }
            
            mod <- dsBaseClient::ds.glm(formula = formula,
                                        data = "baseline_tmp",
                                        family = "gaussian",
                                        maxit = 20,
                                        CI = 0.95,
                                        viewIter = FALSE,
                                        viewVarCov = FALSE,
                                        viewCor = FALSE,
                                        datasources = opals)
            
            # get relevant results and put into data frame
            coefs <- as.data.frame(mod$coefficients)
            coefs$predictor <- rownames(coefs)
            coefs <- coefs[coefs$predictor == predictor, ]
            
            out <- data.frame(outcome = outcome,
                              predictor = predictor,
                              valid_n = mod$Nvalid,
                              beta = coefs[[1]],
                              p.value = coefs[[4]],
                              ci.low = coefs[[5]],
                              ci.high = coefs[[6]])
            
        }
    
    # factor outcome
    } else {
        
        # scale if standardized output is requested (default is TRUE)
        if (standardized == TRUE) {
            dsSwissKnifeClient::dssScale("baseline_tmp",
                                         "baseline_tmp",
                                         datasources = opals)
            
        }
        
        if (covariate == FALSE) {
            
            formula <- as.formula(paste0(outcome, " ~ 1 +", predictor))
            
        } else {
            
            formula <- as.formula(paste0(outcome, " ~ 1 +", paste0(covariate, collapse = "+"), "+", predictor))
            
        }
        
        mod <- dsBaseClient::ds.glm(formula = formula,
                                    data = "baseline_tmp",
                                    family = "gaussian",
                                    maxit = 20,
                                    CI = 0.95,
                                    viewIter = FALSE,
                                    viewVarCov = FALSE,
                                    viewCor = FALSE,
                                    datasources = opals)
        
        # num factor levels
        # (only works for two-level factors?)
        num_levels <- length(tmp[[1]][[3]])
        last_level <- tmp[[1]][[3]][[num_levels]]
        
        # get relevant results and put into data frame
        coefs <- as.data.frame(mod$coefficients)
        coefs$predictor <- rownames(coefs)
        coefs <- coefs[coefs$predictor == paste0(predictor, last_level), ]
        
        out <- data.frame(outcome = outcome,
                          predictor = predictor,
                          valid_n = mod$Nvalid,
                          beta = coefs[[1]],
                          p.value = coefs[[4]],
                          ci.low = coefs[[5]],
                          ci.high = coefs[[6]])
        
    }
    
    return(out)
    
}

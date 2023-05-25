#' Get the correlation between two continuous variables
#'
#' Given the names of two numeric columns in 'baseline', calculates and returns the Pearson correlation and associated metrics. Several checks are performed to make sure the function does not violate privacy rules. The user has the option to subset data by procedure and observation prior to estimating the correlation. The variables may also be reversed, which is useful when negative values are considered 'good', such as percent weight loss. 
#' @param x A character, must correspond to the name of a numerical column in 'baseline'.
#' @param y A character, must correspond to the name of a numerical column in 'baseline'.
#' @param method A character, specifying the type of correlation. Currently only `pearson` is allowed and is thus default. 
#' @param dataframe A character, the name of the federated data frame holding x and y. Defaults to `baseline`.
#' @param keep_procedure A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will only keep rows with this procedure. Defaults to `NA`.
#' @param remove_procedure A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will remove all rows with this procedure. Defaults to `NA`.
#' @param keep_observation A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will only keep rows with this observation. Defaults to `NA`.
#' @param remove_observation A numeric, must be a valid Concept ID for `has_` column created using `dshSophiaCreateBaseline`. Will remove all rows with this observation. Defaults to `NA`.
#' @param reverse_x A Boolean, set to `TRUE` if x should be reversed. Defaults to `FALSE`.
#' @param reverse_y A Boolean, set to `TRUE` if y should be reversed. Defaults to `FALSE`.
#' @return A data frame with the estimated correlation and associated metrics. If privacy checks are not passed, a data frame with NAs will be returned.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect(include = "abos")
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # create a 'baseline' data frame on the federated node, 
#' calculate age at first visit and include 'bypass of stomach' procedure and 'type 2 diabetes' observation
#' dshSophiaCreateBaseline(age_at_first = "visit", procedure_id = 40483096, observation_id = 201826)
#'    
#' # add longitudinal weight measures
#' dshSophiaMergeLongMeas(concept_id = 3038553)
#' 
#' # get correlation between age and weight for subgroup with bypass and T2D
#' dshSophiaGetCor(x = "t1_3038553", y = "age_at_first_visit", keep_procedure = 40483096, keep_observation = 201826)
#' }
#' @import DSOpal opalr httr DSI dsBaseClient dsSwissKnifeClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaGetCor <- function(x, y, 
                            method = "pearson",
                            dataframe = "baseline",
                            keep_procedure = NA, remove_procedure = NA,
                            keep_observation = NA, remove_observation = NA, 
                            reverse_x = FALSE, reverse_y = FALSE) {
    
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        stop("\nNo 'opals' and/or 'nodes_and_cohorts' object found\nYou probably did not run 'dshSophiaConnect' yet")
    }
   
    # make sure no factors are supplied 
    if (dsBaseClient::ds.class(paste0(dataframe, "$", x))[[1]] == "factor") {
        stop("\nFactors are not allowed in x")
    }
    
    if (dsBaseClient::ds.class(paste0(dataframe, "$", y))[[1]] == "factor") {
        stop("\nFactors are not allowed in y")
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
    
    fil <- paste0("c('person_id', ",
                  "'", x, "', ", 
                  "'", y, "'", 
                  rp_fil, ro_fil, kp_fil, ko_fil, ")")

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
    
    # no need to keep going if nrows < 10 at this stage
    nrows <- dsSwissKnifeClient::dssDim("tmp")[[1]][[1]]
    
    if (nrows < 10) {
         
        out <- data.frame(x = x,
                          y = y,
                          method = method,
                          valid.n = nrows,
                          t.val = NA,
                          df = NA,
                          p.val = NA,
                          r = NA,
                          ci.low = NA,
                          ci.high = NA,
                          keep_procedure = keep_procedure,
                          remove_procedure = remove_procedure,
                          keep_observation = keep_observation,
                          remove_observation = remove_observation,
                          reverse_x = reverse_x,
                          reverse_y = reverse_y)
        
    } else {
        
        # reverse x?
        if (reverse_x == TRUE) {
            dsSwissKnifeClient::dssDeriveColumn("tmp", 
                                                x, 
                                                paste0(x, " * -1"),
                                                datasources = opals) 
        }
        
        # reverse y?
        if (reverse_y == TRUE) {
            dsSwissKnifeClient::dssDeriveColumn("tmp", 
                                                y, 
                                                paste0(y, " * -1"),
                                                datasources = opals) 
        }
        
        tryCatch(expr = { 
            
            mod <- dsBaseClient::ds.corTest(x = paste0("tmp$", x),
                                            y = paste0("tmp$", y),
                                            method = method,
                                            conf.level = 0.95,
                                            datasources = opals)[[1]]
            
            out <- data.frame(x = x,
                              y = y,
                              method = method,
                              valid.n = mod[[1]],
                              t.val = mod[[2]][[1]][[1]],
                              df = mod[[2]][[2]][[1]],
                              p.val = mod[[2]][[3]],
                              r = mod[[2]][[4]][[1]],
                              ci.low = mod[[2]][[9]][[1]],
                              ci.high = mod[[2]][[9]][[2]],
                              keep_procedure = keep_procedure,
                              remove_procedure = remove_procedure,
                              keep_observation = keep_observation,
                              remove_observation = remove_observation,
                              reverse_x = reverse_x,
                              reverse_y = reverse_y)
            
        },
        
        error = function(e) {
            
            print(e)
            print(datashield.errors())
            
        })
    }
    
    out$cohort <- strsplit(opals[[1]]@name, "_")[[1]][[1]]
    return(out)
    
}

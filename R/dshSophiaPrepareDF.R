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
#' @import DSOpal opalr httr DSI dsQueryLibrary dsBaseClient dsSwissKnifeClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaPrepareDF <- function(in_df = "baseline",
                               out_df,
                               vars,
                               keep_procedure = NA, 
                               remove_procedure = NA,
                               keep_observation = NA, 
                               remove_observation = NA, 
                               standardize_all = TRUE, 
                               standardize_include = NA,
                               standardize_exclude = NA) {
    
    # check if connected 
    # check if 'baseline' exists
    # check if all variables in 'vars' exist in 'baseline'
    
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
    
    var_fil <- paste0(", '", paste0(vars, collapse = "', '"), "'")
    
    fil <- paste0("c('person_id'",
                  var_fil, rp_fil, ro_fil, kp_fil, ko_fil, ")")
    
    dsSwissKnifeClient::dssSubset("tmp_df",
                                  in_df,
                                  col.filter = fil,
                                  datasources = opals)
    
    # remove procedure?
    if (!is.na(remove_procedure)) {
        dsSwissKnifeClient::dssSubset("tmp_df",
                                      "tmp_df",
                                      row.filter = paste0("has_", remove_procedure, " == 0"),
                                      datasources = opals)
    } 
    
    # keep procedure?
    if (!is.na(keep_procedure)) {
        dsSwissKnifeClient::dssSubset("tmp_df",
                                      "tmp_df",
                                      row.filter = paste0("has_", keep_procedure, " == 1"),
                                      datasources = opals)
    } 

    # remove observation?
    if (!is.na(remove_observation)) {
        dsSwissKnifeClient::dssSubset("tmp_df",
                                      "tmp_df",
                                      row.filter = paste0("has_", remove_observation, " == 0"),
                                      datasources = opals)
    }
    
    # keep observation?
    if (!is.na(keep_observation)) {
        dsSwissKnifeClient::dssSubset("tmp_df",
                                      "tmp_df",
                                      row.filter = paste0("has_", keep_observation, " == 1"),
                                      datasources = opals)
    }
    
    # remove NAs 
    invisible(dsBaseClient::ds.completeCases(x1 = "tmp_df",
                                             newobj = "tmp_df",
                                             datasources = opals))

    # subset to 'vars' and 'person_id' only
    keep_fil <- paste0("c('person_id'", var_fil, ")")
    dsSwissKnifeClient::dssSubset("tmp_df",
                                  "tmp_df",
                                  col.filter = keep_fil,
                                  datasources = opals)

    # scale all?
    if (standardize_all == TRUE) {
        
        dsSwissKnifeClient::dssScale("tmp_df",
                                     "tmp_df",
                                     datasources = opals)
        
    } 

    # scale include?
    if (!is.na(standardize_include)) {
        
        scale_fil <- paste0(", '", paste0(standardize_include, collapse = "', '"), "'")
        scale_fil <- paste0("c('person_id'", scale_fil, ")")
        
        dsSwissKnifeClient::dssSubset("tmp_scale",
                                      "tmp_df",
                                      col.filter = scale_fil,
                                      datasources = opals)
        
        dsSwissKnifeClient::dssScale("tmp_scale",
                                     "tmp_scale",
                                     datasources = opals)
       
        scale_vars <- ds.summary("tmp_scale")[[1]][[4]]
        all_vars <- ds.summary("tmp_df")[[1]][[4]]
        keep_vars <- setdiff(all_vars, scale_vars)

        keep_fil <- paste0(", '", paste0(keep_vars, collapse = "', '"), "'")
        keep_fil <- paste0("c('person_id'", keep_fil, ")")
        
        dsSwissKnifeClient::dssSubset("tmp_df",
                                      "tmp_df",
                                      col.filter = keep_fil,
                                      datasources = opals)
        
        dsSwissKnifeClient::dssJoin(c("tmp_df", "tmp_scale"),
                                    symbol = "tmp_df",
                                    by = "person_id",
                                    join.type = "full",
                                    datasources = opals)

        invisible(dsBaseClient::ds.rm("tmp_scale"))
        
    }

    # scale exclude?
    if (!is.na(standardize_exclude)) {
        
        scale_fil <- paste0(", '", paste0(standardize_exclude, collapse = "', '"), "'")
        scale_fil <- paste0("c('person_id'", scale_fil, ")")
        
        dsSwissKnifeClient::dssSubset("tmp_no_scale",
                                      "tmp_df",
                                      col.filter = scale_fil,
                                      datasources = opals)

        no_scale_vars <- ds.summary("tmp_no_scale")[[1]][[4]]
        all_vars <- ds.summary("tmp_df")[[1]][[4]]
        keep_vars <- setdiff(all_vars, no_scale_vars)

        keep_fil <- paste0(", '", paste0(keep_vars, collapse = "', '"), "'")
        keep_fil <- paste0("c('person_id'", keep_fil, ")")
        
        dsSwissKnifeClient::dssSubset("tmp_df",
                                      "tmp_df",
                                      col.filter = keep_fil,
                              datasources = opals)
         
        dsSwissKnifeClient::dssScale("tmp_df",
                                     "tmp_df",
                                     datasources = opals)
        
        dsSwissKnifeClient::dssJoin(c("tmp_df", "tmp_scale"),
                                    symbol = "tmp_df",
                                    by = "person_id",
                                    join.type = "full",
                                    datasources = opals)

        invisible(dsBaseClient::ds.rm("tmp_no_scale"))
        
    }

    # need to create numeric gender if used as covariate
    if (!any(is.na(covariate))) {
        if ("gender" %in% vars) {
            dsSwissKnifeClient::dssDeriveColumn("tmp_df",
                                                "gender",
                                                "as.numeric(gender) - 1",
                                                datasources = opals)
        }
    }


    # save
    DSI::datashield.workspace_save(opals, out_df)

}

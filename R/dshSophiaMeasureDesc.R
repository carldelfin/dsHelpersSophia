#' Get descriptive information about a variable in the measurement table
#'
#' Given a valid Concept ID the function gathers descriptive information about the corresponding variable in the measurement table and outputs a summary of the results. If the data is longitudinal the output will contain one row per time point.
#' @param dataframe A character, the name of the data frame holding the data. Defaults to `baseline`.
#' @param variable A character, must correspond to a column present in the data frame.
#' @param keep_procedure A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will only keep rows with this procedure. Defaults to `NA`.
#' @param remove_procedure A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will remove all rows with this procedure. Defaults to `NA`.
#' @param keep_observation A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will only keep rows with this observation. Defaults to `NA`.
#' @param remove_observation A numeric, must be a valid Concept ID for a `has_` column created using `dshSophiaCreateBaseline`. Will remove all rows with this observation. Defaults to `NA`.
#' @param keep_gender A character, either `MALE` or `FEMALE`. If supplied, only IDs matching the specified gender will be kept. Defaults to `NA`.
#' @param remove_gender A character, either `MALE` or `FEMALE`. If supplied, all IDs matching the specified gender will be removed. Defaults to `NA`.
#' 
#' @return A data frame with descriptive information.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect(include = "abos")
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # get descriptive information about BMI at timepoint 1, only in males with T2D
#' dshSophiaMeasureDesc(variable = "t1_3038553", keep_observation = 201826, keep_gender = "MALE")
#' }
#' @import DSOpal opalr httr DSI dsQueryLibrary dsBaseClient dsSwissKnifeClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaMeasureDesc <- function(dataframe = "baseline",
                                 variable, 
                                 keep_procedure = NA, remove_procedure = NA,
                                 keep_observation = NA, remove_observation = NA, 
                                 keep_gender = NA, remove_gender = NA) {

    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        stop("\nNo 'opals' and/or 'nodes_and_cohorts' object found\nYou probably did not run 'dshSophiaConnect' yet")
    }
    
    # get measurement unit
    concept_unit <- tryCatch(
        expr = { 
            unit_var <- paste0("unit_", strsplit(as.character(variable), "_")[[1]][[2]])
            concept_unit <- dsBaseClient::ds.levels(paste0(dataframe, "$", unit_var))
            concept_unit <- gsub("unit.", "", concept_unit[[1]][[1]])
            concept_unit
        },
        error = function(e) { 
            print(e)
            e 
        })
   
    if (class(concept_unit)[1] == "simpleError") {
        concept_unit <- NA
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
    
    # remove gender?
    if (!is.na(remove_gender)) {
        rg_fil <- paste0(", 'gender'")
    } else {
        rg_fil <- NULL
    }
    
    # keep gender?
    if (!is.na(keep_gender)) {
        kg_fil <- paste0(", 'gender'")
    } else {
        kg_fil <- NULL
    }
   
    fil <- paste0("c('", variable, "'", rp_fil, ro_fil, kp_fil, ko_fil, rg_fil, kg_fil, ")")

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
    
    # remove gender?
    if (!is.na(remove_gender)) {
        dsSwissKnifeClient::dssSubset("tmp",
                                      "tmp",
                                      row.filter = paste0("gender != '", toupper(keep_gender), "'"),
                                      datasources = opals)
    }
    
    # keep gender?
    if (!is.na(keep_gender)) {
        dsSwissKnifeClient::dssSubset("tmp",
                                      "tmp",
                                      row.filter = paste0("gender == '", toupper(keep_gender), "'"),
                                      datasources = opals)
    }

    # remove NAs 
    invisible(dsBaseClient::ds.completeCases(x1 = "tmp",
                                             newobj = "tmp",
                                             datasources = opals))

    concept_id <- stringr::str_split(variable, "_", n = 3)[[1]][[2]]

    # no need to keep going if summary is empty at this stage
    tmp_summary <- dsBaseClient::ds.summary("tmp")
    
    if (length(tmp_summary[[1]]) == 1) {
        
        out <- data.frame(concept_id = concept_id,
                          concept_unit = concept_unit,
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
        
        tmp_summary <- dsBaseClient::ds.summary(paste0("tmp$", variable))[[1]]
        
        if (length(tmp_summary)[[1]] == 1) {
            
            out <- data.frame(concept_id = concept_id,
                              concept_unit = concept_unit,
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
            
        } else if (tmp_summary[[2]] < 10) {
            
            out <- data.frame(concept_id = concept_id,
                              concept_unit = concept_unit,
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
            
            tmp_var <- dsBaseClient::ds.var(paste0("tmp$", variable))
            tmp_range <- dsSwissKnifeClient::dssRange(paste0("tmp$", variable))
            time <- stringr::str_split(variable, "_", n = 3)[[1]][[1]]
            
            if (length(stringr::str_split(variable, "_", n = 3)[[1]]) == 2) {
                type <- "raw_score"
            } else {
                type <- stringr::str_split(variable, "_", n = 3)[[1]][[3]]
            }
            
            out <- data.frame(concept_id = concept_id,
                              concept_unit = concept_unit,
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

    # remove gender
    if (!is.null(remove_gender)) {
        out$remove_gender <- remove_gender 
    } else {
        out$remove_gender <- NA
    }

    # keep gender
    if (!is.null(keep_gender)) {
        out$keep_gender <- keep_gender 
    } else {
        out$keep_gender <- NA
    }

    return(out)
}

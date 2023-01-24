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
#' @import DSOpal opalr httr DSI dsQueryLibrary dsBaseClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaMergeLongMeas <- function(concept_id, endpoint = "all", difference = "percentage") {

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
    
    if (!difference %in% c("percentage", "raw")) {
        stop("'difference' must be either 'percentage' or 'raw', aborting...")
    }
    
    # make sure the user has specified a concept ID that can be loaded from the measurement table
    tryCatch(
        expr = { 
            # SQL statement
            where_clause <- paste0("measurement_concept_id in ('", concept_id, "')")
            
            # load the measurement table
            dsQueryLibrary::dsqLoad(symbol = "m",
                                   domain = "concept_name",
                                   query_name = "measurement",
                                   where_clause = where_clause,
                                   union = TRUE,
                                   datasources = opals)
        },
        error = function(e) { 
            message("\nUnable to load measurement table, maybe the variable doesn't exist or you forgot to run 'dshSophiaLoad()'?")
    })
    
    # make sure it is ordered by ID and measurement data
    dsSwissKnifeClient::dssSubset("m",
                                  "m",
                                  "order(person_id, measurement_date)")
    
    # check how many time points we have
    dsSwissKnifeClient::dssPivot(symbol = "mw",
                                 what = "m",
                                 value.var = "value_as_number",
                                 formula = "person_id ~ measurement_name",
                                 by.col = "person_id",
                                 fun.aggregate = length,
                                 datasources = opals)
    
    concept_name <- dsBaseClient::ds.summary("mw")[[1]][[4]][2]
    num_timepoints <- dsBaseClient::ds.summary(paste0("mw$", concept_name))[[1]][[3]][[7]]
    
    # are there multiple time points?
    if (num_timepoints > 1) {
        
        # multiple endpoints or just one?
        if (endpoint != "all") {
            
            i <- endpoint
            
            # first time point
            # pivot and take first measurement
            dsSwissKnifeClient::dssPivot("m_t1",
                                         what = "m",
                                         value.var = "value_as_number",
                                         formula = "person_id ~ measurement_name",
                                         by.col = "person_id",
                                         fun.aggregate = "function(x) x[1]",
                                         datasources = opals)
            
            # fix column name and subset
            name1 <- concept_name
            name2 <- gsub("measurement_name.", "", name1)
            name3 <- gsub("\\.", "_", name2)
            name4 <- paste0(tolower(gsub("\\__", "_", name3)), "_t1")
            name5 <- gsub("__", "_", name4)
            
            dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                name5,
                                                name1,
                                                datasources = opals)
            
            dsSwissKnifeClient::dssSubset("m_t1",
                                          "m_t1",
                                          col.filter = paste0("c('person_id', '", name5, "')"),
                                          datasources = opals)
            
            # aggregation function for selecting the ith measurement
            aggr <- paste0("function(x) x[", i, "]")
            
            dsSwissKnifeClient::dssPivot(paste0("m_t", i),
                                         what = "m",
                                         value.var = "value_as_number",
                                         formula = "person_id ~ measurement_name",
                                         by.col = "person_id",
                                         fun.aggregate = aggr,
                                         datasources = opals)
            
            # derive new names based on ith time point
            name4 <- paste0(tolower(gsub("\\__", "_", name3)), "_t", i)
            name5 <- gsub("__", "_", name4)
            
            dsSwissKnifeClient::dssDeriveColumn(paste0("m_t", i),
                                                name5,
                                                name1,
                                                datasources = opals)
            
            dsSwissKnifeClient::dssSubset(paste0("m_t", i),
                                          paste0("m_t", i),
                                          col.filter = paste0("c('person_id', '", name5, "')"),
                                          datasources = opals)
            
            # merge with m_t1
            dsSwissKnifeClient::dssJoin(c(paste0("m_t", i), "m_t1"),
                                        symbol = "m_t1",
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            # calculate difference
            if (difference == "percentage") {
                
                dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                    paste0(name5, "_pct_diff_from_t1"),
                                                    paste0("((", 
                                                           name5,
                                                           " - ", 
                                                           gsub(paste0("t", i), "t1", name5),
                                                           ") / ", 
                                                           gsub(paste0("t", i), "t1", name5), 
                                                           ") * 100"),
                                                    datasources = opals)
                
                dsSwissKnifeClient::dssSubset("m_t1",
                                              "m_t1",
                                              col.filter = paste0("c('person_id', '",
                                                                  paste0(name5, "_pct_diff_from_t1"),
                                                                  "')"),
                                              datasources = opals)
                
            } else {
                
                dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                    paste0(name5, "_minus_t1"), 
                                                    paste0(name5, " - ", gsub(paste0("t", i), "t1", name5)),
                                                    datasources = opals)
                
                dsSwissKnifeClient::dssSubset("m_t1",
                                              "m_t1",
                                              col.filter = paste0("c('person_id', '", 
                                                                  paste0(name4, "_minus_t1"),
                                                                  "')"),
                                              datasources = opals)
            }
            
            # merge with 'baseline'
            dsSwissKnifeClient::dssJoin(c("m_t1", "baseline"),
                                        symbol = "baseline",
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            # remove temporary data frame
            dsBaseClient::ds.rm("m_t1")
            
        } else {
        
            # loop through time points
            for (i in 2:num_timepoints) {
                
                # first time point
                # pivot and take first measurement
                dsSwissKnifeClient::dssPivot("m_t1",
                                             what = "m",
                                             value.var = "value_as_number",
                                             formula = "person_id ~ measurement_name",
                                             by.col = "person_id",
                                             fun.aggregate = "function(x) x[1]",
                                             datasources = opals)
                
                # fix column name and subset
                name1 <- concept_name
                name2 <- gsub("measurement_name.", "", name1)
                name3 <- gsub("\\.", "_", name2)
                name4 <- paste0(tolower(gsub("\\__", "_", name3)), "_t1")
                name5 <- gsub("__", "_", name4)
                
                dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                    name5,
                                                    name1,
                                                    datasources = opals)
                
                dsSwissKnifeClient::dssSubset("m_t1",
                                              "m_t1",
                                              col.filter = paste0("c('person_id', '", name5, "')"),
                                              datasources = opals)
                
                # aggregation function for selecting the ith measurement
                aggr <- paste0("function(x) x[", i, "]")
                
                dsSwissKnifeClient::dssPivot(paste0("m_t", i),
                                             what = "m",
                                             value.var = "value_as_number",
                                             formula = "person_id ~ measurement_name",
                                             by.col = "person_id",
                                             fun.aggregate = aggr,
                                             datasources = opals)
                
                # derive new names based on ith time point
                name4 <- paste0(tolower(gsub("\\__", "_", name3)), "_t", i)
                name5 <- gsub("__", "_", name4)
                
                dsSwissKnifeClient::dssDeriveColumn(paste0("m_t", i),
                                                    name5,
                                                    name1,
                                                    datasources = opals)
                
                dsSwissKnifeClient::dssSubset(paste0("m_t", i),
                                              paste0("m_t", i),
                                              col.filter = paste0("c('person_id', '", name5, "')"),
                                              datasources = opals)
                
                # merge with m_t1
                dsSwissKnifeClient::dssJoin(c(paste0("m_t", i), "m_t1"),
                                            symbol = "m_t1",
                                            by = "person_id",
                                            join.type = "full",
                                            datasources = opals)
                
                # calculate difference
                if (difference == "percentage") {
                    
                    dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                        paste0(name5, "_pct_diff_from_t1"),
                                                        paste0("((", 
                                                               name5,
                                                               " - ", 
                                                               gsub(paste0("t", i), "t1", name5),
                                                               ") / ", 
                                                               gsub(paste0("t", i), "t1", name5), 
                                                               ") * 100"),
                                                        datasources = opals)
                    
                    dsSwissKnifeClient::dssSubset("m_t1",
                                                  "m_t1",
                                                  col.filter = paste0("c('person_id', '",
                                                                      paste0(name5, "_pct_diff_from_t1"),
                                                                      "')"),
                                                  datasources = opals)
                    
                } else {
                    
                    dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                        paste0(name5, "_minus_t1"), 
                                                        paste0(name5, " - ", gsub(paste0("t", i), "t1", name5)),
                                                        datasources = opals)
                    
                    dsSwissKnifeClient::dssSubset("m_t1",
                                                  "m_t1",
                                                  col.filter = paste0("c('person_id', '", 
                                                                      paste0(name4, "_minus_t1"),
                                                                      "')"),
                                                  datasources = opals)
                }
                
                # merge with 'baseline'
                dsSwissKnifeClient::dssJoin(c("m_t1", "baseline"),
                                            symbol = "baseline",
                                            by = "person_id",
                                            join.type = "full",
                                            datasources = opals)
                
                # remove temporary data frame
                dsBaseClient::ds.rm("m_t1")
                
            }
        }
    }

    # remove remaining temporary data frames
    dsBaseClient::ds.rm(c("m", "mw"))
}
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
dshSophiaMergeLongMeas <- function(concept_id, days = TRUE, change = TRUE) {

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
    
    cat("\n\nMerging Concept ID:", concept_id, "\n\n")
    
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
                                 formula = "person_id ~ measurement_concept_id",
                                 by.col = "person_id",
                                 fun.aggregate = length,
                                 datasources = opals)
    
    curr_concept_id <- dsBaseClient::ds.summary("mw")[[1]][[4]][2]
    num_timepoints <- dsBaseClient::ds.summary(paste0("mw$", curr_concept_id))[[1]][[3]][[7]]
    
    # are there multiple time points?
    if (num_timepoints > 1) {

        # first time point
        # pivot and take first measurement
        dsSwissKnifeClient::dssPivot("m_t1",
                                     what = "m",
                                     value.var = "value_as_number",
                                     formula = "person_id ~ measurement_concept_id",
                                     by.col = "person_id",
                                     fun.aggregate = "function(x) x[1]",
                                     datasources = opals)
        
        # fix column name and subset
        name1 <- curr_concept_id
        name2 <- gsub("measurement_concept_id.", "", name1)
        name3 <- paste0("t1_", name2)
        
        dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                            name3,
                                            name1,
                                            datasources = opals)
        
        dsSwissKnifeClient::dssSubset("m_t1",
                                      "m_t1",
                                      col.filter = paste0("c('person_id', '", name3, "')"),
                                      datasources = opals)
    
        # loop through time points
        for (i in 2:num_timepoints) {
            
            # derive new names based on ith time point
            name3 <- paste0("t", i, "_", name2)
            
            # time difference requested?
            if (days == TRUE) {
                
                aggr <- paste0("function(x) as.numeric(as.Date(x[", i, "]) - as.Date(x[1]))")
                
                dsSwissKnifeClient::dssPivot(symbol = "tdiff",
                                             what = "m",
                                             value.var = "measurement_date",
                                             formula = "person_id ~ measurement_name",
                                             by.col = "person_id",
                                             fun.aggregate = eval(parse(text = aggr)),
                                             datasources = opals)
                
                concept_name <- dsBaseClient::ds.summary("tdiff")[[1]][[4]][[2]]
                
                # remove missing
                dsBaseClient::ds.completeCases(x1 = "tdiff",
                                               newobj = "tdiff",
                                               datasources = opals)
                
                # remove unrealistic values
                # (8000 is approx. number of days from 2000-01-01 to today)
                dsSwissKnifeClient::dssSubset("tdiff",
                                              "tdiff",
                                              row.filter = paste0("tdiff$", concept_name, " < 8000"),
                                              datasources = opals)
                
                # create new time from t1 column
                dsSwissKnifeClient::dssDeriveColumn("tdiff",
                                                    paste0(name3, "_days_since_t1"),
                                                    concept_name,
                                                    datasources = opals)
                
                # subset
                dsSwissKnifeClient::dssSubset("tdiff",
                                              "tdiff",
                                              col.filter = paste0("c('person_id', '", name3, "_days_since_t1')"),
                                              datasources = opals)
            }
            
            # aggregation function for selecting the ith measurement
            aggr <- paste0("function(x) x[", i, "]")
            
            dsSwissKnifeClient::dssPivot(paste0("m_t", i),
                                         what = "m",
                                         value.var = "value_as_number",
                                         formula = "person_id ~ measurement_concept_id",
                                         by.col = "person_id",
                                         fun.aggregate = aggr,
                                         datasources = opals)
            
            dsSwissKnifeClient::dssDeriveColumn(paste0("m_t", i),
                                                name3,
                                                name1,
                                                datasources = opals)
            
            dsSwissKnifeClient::dssSubset(paste0("m_t", i),
                                          paste0("m_t", i),
                                          col.filter = paste0("c('person_id', '", name3, "')"),
                                          datasources = opals)
            
            # merge with m_t1
            dsSwissKnifeClient::dssJoin(c(paste0("m_t", i), "m_t1"),
                                        symbol = "m_t1",
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            # merge tdiff with m_t1 if requested 
            if (days == TRUE) {
                
                dsSwissKnifeClient::dssJoin(c("m_t1", "tdiff"),
                                            symbol = "m_t1",
                                            by = "person_id",
                                            join.type = "full",
                                            datasources = opals)
    
                dsBaseClient::ds.rm("tdiff")

            }
            
            # calculate change?
            if (change == TRUE) {
                
                dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                    paste0(name3, "_pct_change_from_t1"),
                                                    paste0("((", 
                                                           name3,
                                                           " - ", 
                                                           gsub(paste0("t", i), "t1", name3),
                                                           ") / ", 
                                                           gsub(paste0("t", i), "t1", name3), 
                                                           ") * 100"),
                                                    datasources = opals)
                
                dsSwissKnifeClient::dssDeriveColumn("m_t1",
                                                    paste0(name3, "_raw_change_from_t1"), 
                                                    paste0(name3, " - ", gsub(paste0("t", i), "t1", name3)),
                                                    datasources = opals)
                
            }

        }
            
        # merge with 'baseline'
        dsSwissKnifeClient::dssJoin(c("m_t1", "baseline"),
                                    symbol = "baseline",
                                    by = "person_id",
                                    join.type = "full",
                                    datasources = opals)
        
    }

    # remove temporary data frames
    dsBaseClient::ds.rm(c("m_t1", "m", "mw"))
}

#' Create a baseline data frame on the federated node
#'
#' Given a vector of valid measurement table Concept IDs the function creates a 'baseline' data frame on the federated node. Here, 'baseline' means the first measurement available, and as such may be different for different Concept IDs. The function also creates an approximate 'age_baseline' variable and merged the baseline data with gender if available in the person table.
#' @return A data frame.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect()
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # create a 'baseline' data frame on the federated node
#' dshSophiaCreateBaseline(concept_id = c(3038553, 3025315, 37020574))
#' 
#' # check result
#' dsBaseClient::ds.summary("baseline")
#' }
#' @import DSOpal opalr httr DSI dsQueryLibrary dsBaseClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaMergeLongMeas <- function(concept_id) {

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
    
    # make sure the user has specified a concept ID that can be loaded from the measurement table
    tryCatch(
        expr = { 
            # SQL statement
            where_clause <- paste0("measurement_concept_id in ('", concept_id, "')")
            
            # load the measurement table
            dsqLoad(symbol = "m",
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

    # loop through time points and gather results
    for (i in 1:num_timepoints) {
        
        # aggregation function for selecting the ith measurement
        aggr <- paste0("function(x) x[", i, "]")
        
        # pivot and take ith measurement
        dsSwissKnifeClient::dssPivot(symbol = paste0("m_t", i),
                                     what = "m",
                                     value.var = "value_as_number",
                                     formula = "person_id ~ measurement_name",
                                     by.col = "person_id",
                                     fun.aggregate = eval(parse(text = aggr)),
                                     datasources = opals)
        
        # fix column name and subset
        name1 <- concept_name
        name2 <- gsub("measurement_name.", "", name1)
        name3 <- gsub("\\.", "_", name2)
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
        
        # merge with 'baseline'
        dsSwissKnifeClient::dssJoin(c(paste0("m_t", i), "baseline"), 
                                    symbol = "baseline",
                                    by = "person_id",
                                    join.type = "inner",
                                    datasources = opals)
        
        # calculate actual difference and percent change
        if (i > 1) {
            dsSwissKnifeClient::dssDeriveColumn("baseline",
                                                paste0(name5, "_minus_t1"), 
                                                paste0(name5, " - ", gsub(paste0("t", i), "t1", name5)),
                                                datasources = opals)
            
            dsSwissKnifeClient::dssDeriveColumn("baseline",
                                                paste0(name5, "_pct_diff_from_t1"),
                                                paste0("((", name5, " - ", gsub(paste0("t", i), "t1", name5), ") / ", gsub(paste0("t", i), "t1", name5), ") * 100"),
                                                datasources = opals)
        }
    }
}

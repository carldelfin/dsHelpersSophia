#' Get descriptive information about a variable in the measurement table
#'
#' Given a valid Concept ID the function gathers descriptive information about the corresponding variable in the measurement table and outputs a summary of the results. If the data is longitudinal the output will contain one row per time point.
#' @return A data frame.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect()
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # get variable descriptives for BMI
#' df <- dshSophiaMeasureDesc(concept_id = 3038553)
#' }
#' @import DSOpal opalr httr DSI dsQueryLibrary dsBaseClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaMeasureDesc <- function(concept_id) {

    # if there is not an 'opals' or an 'nodes_and_cohorts' object in the Global environment,
    # the user probably did not run dshSophiaConnect() yet. Here the user may do so, after 
    # being promted for username and password.
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        cat("")
        cat("No 'opals' and/or 'nodes_and_cohorts' object found\n")
        cat("You probably did not run 'dshSophiaConnect' yet, do you wish to do that now?\n")
        switch(menu(c("Yes", "No (abort)")) + 1,
               cat("Test"), 
               dshSophiaPrompt(),
               stop("Aborting..."))
    }
    
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
    
    # check how many timepoints we have
    dsSwissKnifeClient::dssPivot(symbol = "mw",
                                 what = "m",
                                 value.var = "value_as_number",
                                 formula = "person_id ~ measurement_name",
                                 by.col = "person_id",
                                 fun.aggregate = length,
                                 datasources = opals)
    
    concept_name <- dsBaseClient::ds.summary("mw")[[1]][[4]][2]
    num_timepoints <- dsBaseClient::ds.summary(paste0("mw$", concept_name))[[1]][[3]][[7]]
    
    # temporary results storage
    res <- NULL
    
    # loop through timepoints and gather results
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
        
        # remove missing
        dsBaseClient::ds.completeCases(x1 = paste0("m_t", i),
                                       newobj = paste0("m_t", i),
                                       datasources = opals)
        
        # calculate average time difference
        if (i > 1) {
            
            # aggregation function for time difference
            aggr <- paste0("function(x) as.numeric(as.Date(x[", i, "]) - as.Date(x[1]))")
            
            dsSwissKnifeClient::dssPivot(symbol = "tdiff",
                                         what = "m",
                                         value.var = "measurement_date",
                                         formula = "person_id ~ measurement_name",
                                         by.col = "person_id",
                                         fun.aggregate = eval(parse(text = aggr)),
                                         datasources = opals)
            
            # remove any missing (there shouldn't be any)
            dsBaseClient::ds.completeCases(x1 = "tdiff",
                                           newobj = "tdiff",
                                           datasources = opals)
            
            # only time differences where the baseline value is > 0 UNIX time is allowed
            # (this is due to many cohorts using datetime 1900-01-01 as placeholder for missing)
            dsSwissKnifeClient::dssSubset(symbol = "tdiff",
                                          what = "tdiff",
                                          row.filter = paste0("tdiff$", concept_name, " < ", as.numeric(Sys.Date() - as.Date("1970-01-01"))),
                                          datasources = opals)
            
            # mean number of days/months between timepoint 1 and timepoint i
            mean_days_from_baseline <- dsBaseClient::ds.summary(paste0("tdiff$", concept_name))[[1]][[3]][[4]]
            mean_months_from_baseline <- round(mean_days_from_baseline / 30, 0)
            
        } else {
            
            # if we're at timepoint 1 then these are zero
            mean_days_from_baseline <- 0
            mean_months_from_baseline <- 0
            
        }
        
        # gather all summary data into data frame
        tmp <- dsBaseClient::ds.summary(paste0("m_t", i, "$", concept_name))[[1]][[3]]
        
        out <- data.frame(time = i,
                          mean_days_from_baseline = mean_days_from_baseline,
                          mean_months_from_baseline = mean_months_from_baseline,
                          n = dsSwissKnifeClient::dssVar(paste0("m_t", i, "$", concept_name))[[1]][[2]],
                          mean = tmp[[8]],
                          sd = sqrt(dsSwissKnifeClient::dssVar(paste0("m_t", i, "$", concept_name))[[1]][[1]]),
                          median = tmp[[4]],
                          q1 = tmp[[3]],
                          q3 = tmp[[5]],
                          iqr = tmp[[5]] - tmp[[3]],
                          min = dsSwissKnifeClient::dssRange(paste0("m_t", i, "$", concept_name))[[1]][[1]][[1]],
                          max = dsSwissKnifeClient::dssRange(paste0("m_t", i, "$", concept_name))[[1]][[1]][[2]])
        
        res <- rbind(res, out)
    }
    
    # tidy up the output
    res <- res |>
        dplyr::mutate(concept_id = concept_id,
                      concept_name = gsub("measurement_name.", "", concept_name),
                      mean_pct_change = -(100 - (mean/mean[1L]) * 100)) |>
        dplyr::select(concept_id, concept_name, time, 
                      mean_days_from_baseline, mean_months_from_baseline,
                      n, mean, mean_pct_change, sd, median, 
                      q1, q3, iqr, min, max)
    
    return(res)

}

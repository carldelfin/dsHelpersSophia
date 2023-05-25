#' Merge a (longitudinal) variable from the Measurement table with an existing federated data frame
#'
#' Given a valid Measurement table Concept ID, merges that variable (in wide format) with an existing data frame on the federated node. If the variable is available across multiple time points, all time points may be included, and the raw difference plus percentage change from time point 1 to time point X can be calculated, along with the number of days since baseline. Units can also be extracted, and outliers can be removed prior to merging.
#' @param dataframe A character, the name of the data frame holding the data. Defaults to `baseline`.
#' @param concept_id A numeric, must be a valid Concept ID from the Measurement table. The resulting variable will named after the Concept ID and prefixed with `tx_`, with `x` referring to timepoint (e.g., baseline measurements will be named `t1_xxxxxx`).
#' @param change A Boolean, if `TRUE` (the default) and multiple timepoints are available, raw and percentage change between timepoints will be calculated. 
#' @param days A Boolean, if `TRUE` (the default) and multiple timepoints are available, the number of days between `t1` and `tx` will be calculated. 
#' @param limit_time A numeric, can be used to constrain variables measured across multiple timepoints to a specific number of timepoints (e.g. if `1`, only the first timepoint will be included). Defaults to `NULL` such that all available timepoints are included.
#' @param unit A Boolean, if `TRUE` (the default) the unit used for the variable is extracted. 
#' @param outlier_sd A numeric, if supplied removes all rows where the variable value is `outlier_sd` times the standard deviation from the mean (e.g. if `2`, all rows with a value above or below 2 times the standard deviation from the mean are removed). Defaults to `NULL`.
#' @param outlier_iqr A numeric, if supplied removes all rows where the variable value is `outlier_iqr` times the interquartile range from the mean (e.g. if `2`, all rows with a value above or below 2 times the interquartile range from the mean are removed). Defaults to `NULL`.
#' @return Nothing, the federated data frame is appended.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect(include )
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
#' dsBaseClient::ds.summary(dataframe)
#' }
#' @import DSOpal opalr httr DSI dsQueryLibrary dsBaseClient dsSwissKnifeClient dplyr
#' @importFrom utils menu 
#' @export
dshSophiaMergeLongMeas <- function(dataframe = "baseline",
                                   concept_id, 
                                   change = TRUE, 
                                   days = TRUE, 
                                   limit_time = NULL, 
                                   unit = TRUE, 
                                   outlier_sd = NULL, 
                                   outlier_iqr = NULL) {
    
    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        stop("\nNo 'opals' or 'nodes_and_cohorts' object found! Did you forget to run 'dshSophiaConnect'?")
    }
    
    if (!is.null(outlier_sd) && !is.null(outlier_iqr)) {
        stop("Outlier removal using both SD and IQR not possible!")
    }
 
    cat("\n\nMerging Concept ID:", concept_id, "\n\n")
    
    # make sure the user has specified a concept ID that can be loaded from the measurement table
    tryCatch(
        expr = { 
            where_clause <- paste0("measurement_concept_id in ('", concept_id, "')")
            
            invisible(dsQueryLibrary::dsqLoad(symbol = "m",
                                              domain = "concept_name",
                                              query_name = "measurement",
                                              where_clause = where_clause,
                                              union = TRUE,
                                              datasources = opals))

            # make sure it is ordered by ID and measurement date
            dsSwissKnifeClient::dssSubset("m",
                                          "m",
                                          "order(person_id, measurement_date)")
        },
        error = function(e) { 
            message("\nUnable to load measurement table, maybe the variable doesn't exist or you forgot to run 'dshSophiaLoad()'?")
        })
   
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

    # outlier removal
    if (!is.null(outlier_sd)) {

        tmp_sd <- sqrt(dsBaseClient::ds.var(paste0("m_t1$", name3))[[1]][[1]])
        tmp_mean <- dsBaseClient::ds.summary(paste0("m_t1$", name3))[[1]][[3]][[8]]

        upper_limit <- tmp_mean + (outlier_sd * tmp_sd)
        lower_limit <- tmp_mean - (outlier_sd * tmp_sd)

        dsSwissKnifeClient::dssSubset("m_t1",
                                      "m_t1",
                                      row.filter = paste0(name3, " < ", upper_limit, " & ", name3, " > ", lower_limit),
                                      datasources = opals)

    }

    if (!is.null(outlier_iqr)) {

        tmp_summary <- dsBaseClient::ds.summary(paste0("m_t1$", name3))[[1]]
        tmp_q1 <- tmp_summary[[3]][[3]]
        tmp_q3 <- tmp_summary[[3]][[5]]
        tmp_iqr <- tmp_summary[[3]][[5]] - tmp_summary[[3]][[3]]

        upper_limit <- tmp_q3 + (outlier_iqr * tmp_iqr)
        lower_limit <- tmp_q1 - (outlier_iqr * tmp_iqr)

        dsSwissKnifeClient::dssSubset("m_t1",
                                      "m_t1",
                                      row.filter = paste0(name3, " < ", upper_limit, " & ", name3, " > ", lower_limit),
                                      datasources = opals)

    }
    
    # merge with 'dataframe'
    dsSwissKnifeClient::dssJoin(c(dataframe, "m_t1"),
                                symbol = dataframe,
                                by = "person_id",
                                join.type = "left",
                                datasources = opals)
            
    invisible(dsBaseClient::ds.rm("m_t1"))

    if (!is.null(limit_time)) {
        num_timepoints <- limit_time
    }

    if (num_timepoints > 1) {
        
        # loop through time points
        for (i in 2:num_timepoints) {
            
            # derive new name based on ith time point
            name3 <- paste0("t", i, "_", name2)
             
            # aggregation function for selecting the ith measurement
            aggr <- paste0("function(x) x[", i, "]")
            
            dsSwissKnifeClient::dssPivot("m_tmp",
                                         what = "m",
                                         value.var = "value_as_number",
                                         formula = "person_id ~ measurement_concept_id",
                                         by.col = "person_id",
                                         fun.aggregate = aggr,
                                         datasources = opals)
            
            dsSwissKnifeClient::dssDeriveColumn("m_tmp",
                                                name3,
                                                name1,
                                                datasources = opals)
            
            dsSwissKnifeClient::dssSubset("m_tmp",
                                          "m_tmp", 
                                          col.filter = paste0("c('person_id', '", name3, "')"),
                                          datasources = opals)

            # outlier removal
            if (!is.null(outlier_sd)) {

                tmp_sd <- sqrt(dsBaseClient::ds.var(paste0("m_tmp$", name3))[[1]][[1]])
                tmp_mean <- dsBaseClient::ds.summary(paste0("m_tmp$", name3))[[1]][[3]][[8]]

                upper_limit <- tmp_mean + (outlier_sd * tmp_sd)
                lower_limit <- tmp_mean - (outlier_sd * tmp_sd)

                dsSwissKnifeClient::dssSubset("m_tmp",
                                              "m_tmp",
                                              row.filter = paste0(name3, " < ", upper_limit, " & ", name3, " > ", lower_limit),
                                              datasources = opals)

            }

            if (!is.null(outlier_iqr)) {

                tmp_summary <- dsBaseClient::ds.summary(paste0("m_tmp$", name3))[[1]]
                tmp_q1 <- tmp_summary[[3]][[3]]
                tmp_q3 <- tmp_summary[[3]][[5]]
                tmp_iqr <- tmp_summary[[3]][[5]] - tmp_summary[[3]][[3]]

                upper_limit <- tmp_q3 + (outlier_iqr * tmp_iqr)
                lower_limit <- tmp_q1 - (outlier_iqr * tmp_iqr)
                
                dsSwissKnifeClient::dssSubset("m_tmp",
                                              "m_tmp",
                                              row.filter = paste0(name3, " < ", upper_limit, " & ", name3, " > ", lower_limit),
                                              datasources = opals)

            }

            # calculate days from baseline? 
            if (days == TRUE) {

                t_aggr <- paste0("function(x) as.numeric(as.Date(x[", i, "]) - as.Date(x[1]))")

                dsSwissKnifeClient::dssPivot(symbol = "tdiff",
                                             what = "m",
                                             value.var = "measurement_date",
                                             formula = "person_id ~ measurement_name",
                                             by.col = "person_id",
                                             fun.aggregate = eval(parse(text = t_aggr)),
                                             datasources = opals)

                # subset to IDs in m_tmp
                dsSwissKnifeClient::dssSubset("tdiff",
                                              "tdiff",
                                              row.filter = 'tdiff$person_id %in% m_tmp$person_id',
                                              datasources = opals)

                concept_name <- dsBaseClient::ds.summary("tdiff")[[1]][[4]][[2]]

                # remove unrealistic values
                # (8000 is approx. number of days from 2000-01-01 to today)
                dsSwissKnifeClient::dssSubset("tdiff",
                                              "tdiff",
                                              row.filter = paste0("tdiff$", concept_name, " < 8000"),
                                              datasources = opals)
                
                # remove missing
                dsBaseClient::ds.completeCases(x1 = "tdiff",
                                               newobj = "tdiff",
                                               datasources = opals)

                # create new time from t1 column
                dsSwissKnifeClient::dssDeriveColumn("tdiff",
                                                    paste0(name3, "_days_since_t1"),
                                                    concept_name,
                                                    datasources = opals)

                # subset to relevant columns
                dsSwissKnifeClient::dssSubset("tdiff",
                                              "tdiff",
                                              col.filter = paste0("c('person_id', '", name3, "_days_since_t1')"),
                                              datasources = opals)

                # merge 
                dsSwissKnifeClient::dssJoin(c("m_tmp", "tdiff"),
                                            symbol = "m_tmp",
                                            by = "person_id",
                                            join.type = "left",
                                            datasources = opals)

                invisible(dsBaseClient::ds.rm("tdiff"))

            }

            # merge
            dsSwissKnifeClient::dssJoin(c(dataframe, "m_tmp"),
                                        symbol = dataframe,
                                        by = "person_id",
                                        join.type = "left",
                                        datasources = opals)

            invisible(dsBaseClient::ds.rm("m_tmp"))

            # calculate change?
            if (change == TRUE) {
               
                dsSwissKnifeClient::dssDeriveColumn(dataframe,
                                                    paste0(name3, "_pct_change_from_t1"),
                                                    paste0("((", 
                                                           name3,
                                                           " - ", 
                                                           gsub(paste0("t", i), "t1", name3),
                                                           ") / ", 
                                                           gsub(paste0("t", i), "t1", name3), 
                                                           ") * 100"),
                                                    datasources = opals)
               
                dsSwissKnifeClient::dssDeriveColumn(dataframe,
                                                    paste0(name3, "_raw_change_from_t1"), 
                                                    paste0(name3, " - ", gsub(paste0("t", i), "t1", name3)),
                                                    datasources = opals)
                if (i > 2) {

                    dsSwissKnifeClient::dssDeriveColumn(dataframe,
                                                        paste0(name3, "_pct_change_from_t", i - 1),
                                                        paste0("((", 
                                                               name3,
                                                               " - ", 
                                                               gsub(paste0("t", i), paste0("t", i - 1), name3),
                                                               ") / ", 
                                                               gsub(paste0("t", i), paste0("t", i - 1), name3),
                                                               ") * 100"),
                                                        datasources = opals)

                    dsSwissKnifeClient::dssDeriveColumn(dataframe,
                                                        paste0(name3, "_raw_change_from_t", i - 1), 
                                                        paste0(name3, " - ", gsub(paste0("t", i), paste0("t", i - 1), name3)),
                                                        datasources = opals)

                }
            }
        }
    }

    if (unit == TRUE) {

        dsSwissKnifeClient::dssPivot(symbol = "mu",
                                     what = "m",
                                     value.var = "unit",
                                     formula = "person_id ~ unit",
                                     by.col = "person_id",
                                     fun.aggregate = "function(x) x[1]",
                                     datasources = opals)

        invisible(dsSwissKnifeClient::dssColNames("mu", 
                                                  value = c("person_id", paste0("unit_", name2))))

        # merge with 'dataframe'
        dsSwissKnifeClient::dssJoin(c(dataframe, "mu"),
                                    symbol = dataframe,
                                    by = "person_id",
                                    join.type = "left",
                                    datasources = opals)

        invisible(dsBaseClient::ds.rm("mu"))

    }

    invisible(dsBaseClient::ds.rm("m"))

}

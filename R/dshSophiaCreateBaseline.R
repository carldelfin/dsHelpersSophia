#' Create a baseline data frame on the federated node
#'
#' Given a vector of valid measurement table Concept IDs the function creates a 'baseline' data frame on the federated node. Here, 'baseline' means the first measurement available, and as such may be different for different Concept IDs. The function also creates an approximate 'age_baseline' variable and merged the baseline data with gender if available in the person table.
#' @return A federated data frame named 'baseline'.
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
dshSophiaCreateBaseline <- function(concept_id) {

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
    
    # make sure the user has specified some concept IDs
    if (is.null(concept_id)) { 
        stop("No measurement table concept IDs specified, aborting...")
    }
    
    # ----------------------------------------------------------------------------------------------
    # get date of first measurement (i.e. the baseline)
    # ----------------------------------------------------------------------------------------------
    
    # load measurement table
    dsQueryLibrary::dsqLoad(symbol = "m",
                            domain = "concept_name",
                            query_name = "measurement",
                            union = TRUE,
                            datasources = opals)
    
    # order by date and person id
    dsSwissKnifeClient::dssSubset("m",
                                  "m",
                                  "order(person_id, measurement_date)",
                                  async = TRUE)
    
    # derive numeric date columns
    dsSwissKnifeClient::dssDeriveColumn("m", 
                                        "measurement_date_n",
                                        "as.numeric(as.Date(measurement_date))")
    
    dsSwissKnifeClient::dssDeriveColumn("m", 
                                        "f", 
                                        "'irst_measurement_dat.e'")
    
    # pivot and select first measurement, which is now the first available date
    dsSwissKnifeClient::dssPivot(symbol = "m_date",
                                 what = "m",
                                 value.var = "measurement_date_n",
                                 formula = "person_id ~ f",
                                 by.col = "person_id",
                                 fun.aggregate = function(x) x[1],
                                 async = TRUE,
                                 datasources = opals)
    
    # remove temporary data frame
    dsBaseClient::ds.rm("m")
    
    # ----------------------------------------------------------------------------------------------
    # get baseline clinical variables from measurement table
    # ----------------------------------------------------------------------------------------------
    
    # create SQL statement
    where_clause <- paste(concept_id, collapse = ",")
    where_clause <- paste0("measurement_concept_id in (", where_clause, ")")
    
    # load temporary measurement data
    dsQueryLibrary::dsqLoad(symbol = "m_tmp",
                            domain = "concept_name",
                            query_name = "measurement",
                            where_clause = where_clause,
                            union = TRUE,
                            datasources = opals)
    
    # make sure it is ordered by ID and measurement data
    dsSwissKnifeClient::dssSubset("m_tmp",
                                  "m_tmp", 
                                  "order(person_id, measurement_date)")
    
    # pivot to select the first measurement
    dsSwissKnifeClient::dssPivot(symbol = "m_tmp_t1",
                                 what = "m_tmp",
                                 value.var = "value_as_number",
                                 formula = "person_id ~ measurement_name",
                                 by.col = "person_id",
                                 fun.aggregate = function(x) x[1],
                                 datasources = opals)
    
    # join with date, create a new data frame called 'baseline'
    dsSwissKnifeClient::dssJoin(c("m_date", "m_tmp_t1"), 
                                symbol = "baseline",
                                by = "person_id",
                                join.type = "inner",
                                datasources = opals)
    
    # remove temporary data frames
    dsBaseClient::ds.rm("m_tmp")
    dsBaseClient::ds.rm("m_tmp_t1")
    
    # --------------------------------------------------------------------------------------------------
    # add age and gender from person table
    # --------------------------------------------------------------------------------------------------
    
    # load person table
    dsQueryLibrary::dsqLoad(symbol = "p",
                            domain = "concept_name",
                            query_name = "person",
                            union = TRUE,
                            datasources = opals,
                            async = TRUE)
    
    # join with 'date' data frame
    dsSwissKnifeClient::dssJoin(c("p", "m_date"),
                                symbol = "p_age", 
                                by = "person_id", 
                                join.type = "inner")
    
    # derive approximate age at baseline
    dsSwissKnifeClient::dssDeriveColumn("p_age",
                                        "age_baseline", 
                                        "round((f.irst_measurement_dat.e - as.numeric(as.Date(birth_datetime)))/365)")
    
    # keep only columns we need
    dsSwissKnifeClient::dssSubset("p_age",
                                  "p_age",
                                  col.filter = "c('person_id', 'gender', 'age_baseline')",
                                  datasources = opals)
    
    # join with 'baseline'
    dsSwissKnifeClient::dssJoin(c("p_age", "baseline"), 
                                symbol = "baseline",
                                by = "person_id",
                                join.type = "inner",
                                datasources = opals)
    
    # remove temporary data frames
    dsBaseClient::ds.rm("p_age")
    dsBaseClient::ds.rm("p")
    dsBaseClient::ds.rm("m_date")
    
    # --------------------------------------------------------------------------------------------------
    # clean up column names
    # --------------------------------------------------------------------------------------------------
    
    column_names <- dsBaseClient::ds.colnames("baseline")
    column_names <- grep("measurement_name", column_names[[1]], value = TRUE)
    
    for (i in column_names) {
        
        name1 <- i
        name2 <- gsub("measurement_name.", "", name1)
        name3 <- gsub("\\.", "_", name2)
        name4 <- tolower(gsub("\\__", "_", name3))
        name5 <- gsub("__", "_", name4)
        
        dsSwissKnifeClient::dssDeriveColumn("baseline",
                                            name5,
                                            name1,
                                            datasources = opals)
    }
    
    dsSwissKnifeClient::dssSubset("baseline",
                                  "baseline",
                                  col.filter = '!grepl("measurement_name", colnames(baseline))',
                                  datasources = opals)
    
    dsSwissKnifeClient::dssSubset("baseline",
                                  "baseline",
                                  col.filter = '!grepl("f.irst_measurement_dat.e", colnames(baseline))',
                                  datasources = opals)

}
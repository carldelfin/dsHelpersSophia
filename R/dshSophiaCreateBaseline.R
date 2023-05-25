#' Create a baseline data frame on the federated node
#'
#' Creates a 'baseline' data frame on the federated node(s). Here, 'baseline' simply means baseline characteristics such as gender, age, and various procedures and observations. The 'baseline' data frame can be used as a stepping stone for creating more elaborate data frames.
#' @param name A character, the name of the resulting data frame. Defaults to `baseline`.
#' @param procedure_id A numeric, must be a valid Concept ID in the Procedure table, that is coded as either present or not present. If supplied, will create a factor column named after the Concept ID, prefixed with `has_`. A value of 1 will mean that the procedure is present, and a value of 0 will mean that the procedure is not present or is missing. Defaults to `NULL`.
#' @param observation_id A numeric, must be a valid Concept ID in the Observation table, that is coded as either present or not present. If supplied, will create a factor column named after the Concept ID, prefixed with `has_`. A value of 1 will mean that the observation is present, and a value of 0 will mean that the observation is not present or is missing. Defaults to `NULL`.
#' @param age_at_first Either a character `visit` or a valid Concept ID from the Measurement table. If `visit` then age at first visit is calculated. If a Concept ID, then the age at first available measurement of that Concept ID is calculated. Cannot be used at the same time as `age_at_year`. Defaults to `NULL`.
#' @param age_at_year A numeric, corresponding to a year (e.g. `2000`). The age at that specific year is calculated. Cannot be used at the same time as `age_at_first`. Defaults to `NULL`.
#' @return A federated data frame named 'baseline'.
#' @examples
#' \dontrun{
#' # connect to the federated system
#' dshSophiaConnect(include = "abos")
#'
#' # load database resources
#' dshSophiaLoad()
#'
#' # create a 'baseline' data frame on the federated node
#' dshSophiaCreateBaseline(procedure_id = c(3038553, 3025315, 37020574),
#' observation_id = 201826, 
#' age_at_first = "visit")
#' 
#' # check result
#' dsBaseClient::ds.summary(name)
#' }
#' @import DSOpal opalr httr DSI dsQueryLibrary dsResource dsBaseClient dplyr
#' @export
dshSophiaCreateBaseline <- function(name = name,
                                    procedure_id = NULL, 
                                    observation_id = NULL, 
                                    age_at_first = NULL, 
                                    age_at_year = NULL) {

    if (exists("opals") == FALSE || exists("nodes_and_cohorts") == FALSE) {
        stop("\nNo 'opals' and/or 'nodes_and_cohorts' object found\nYou probably did not run 'dshSophiaConnect' yet")
    }
   
    if (!is.null(age_at_year) & !is.null(age_at_first)) {
        stop("\n'age_at_year' and 'age_at_first' cannot both be used")
    }
    
    # load person table
    dsQueryLibrary::dsqLoad(symbol = name,
                            domain = "concept_name",
                            query_name = "person",
                            union = TRUE,
                            datasources = opals,
                            async = TRUE)
    
    # keep only columns we need
    dsSwissKnifeClient::dssSubset(name,
                                  name,
                                  col.filter = "colnames(baseline) %in% c('person_id', 'gender', 'year_of_birth', 'birth_datetime')",
                                  datasources = opals)
    
    if (!is.null(procedure_id)) {
        
        for (i in procedure_id) {
            
            p_clause <- paste0("procedure_concept_id in ('", i, "')")
            
            dsQueryLibrary::dsqLoad(symbol = "p",
                                    domain = "concept_name",
                                    query_name = "procedure_occurrence",
                                    where_clause = p_clause,
                                    union = TRUE,
                                    datasources = opals)
            
            dsSwissKnifeClient::dssDeriveColumn("p",
                                                paste0("tmp_", i), 
                                                "1")

            dsSwissKnifeClient::dssSubset("p",
                                          "p",
                                          col.filter = paste0("c('person_id', 'tmp_", i, "')"),
                                          datasources = opals)
            
            # join into temporary data frame 'tmp'
            dsSwissKnifeClient::dssJoin(c("p", name),
                                        symbol = "tmp",
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            # change NAs into 0s
            dsBaseClient::ds.replaceNA(paste0("tmp$tmp_", i), 
                                       0, 
                                       paste0("tmp2_", i))
            
            dsBaseClient::ds.asFactor(paste0("tmp$tmp2_", i), "tmp2")
            
            dsSwissKnifeClient::dssDeriveColumn("tmp",
                                                paste0("has_", i), 
                                                "tmp2")
            
            dsSwissKnifeClient::dssSubset("tmp",
                                          "tmp",
                                          col.filter = paste0("c('person_id', 'has_", i, "')"),
                                          datasources = opals)
            
            # merge with 'name'
            dsSwissKnifeClient::dssJoin(c("tmp", name),
                                        symbol = name,
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            invisible(dsBaseClient::ds.rm("tmp"))
        }
    } 
    
    if (!is.null(observation_id)) {
        
        for (i in observation_id) {
            
            o_clause <- paste0("observation_concept_id in ('", i, "')")
            connection_name <- gsub("\\.", "_", DSI::datashield.resources(opals)[[1]])

            dsResource::dsrAssign(symbol = "o",
                                  table = "observation",
                                  where_clause = o_clause,
                                  db_connection = connection_name,
                                  datasources = opals)

            dsSwissKnifeClient::dssSubset("o",
                                          "o",
                                          row.filter = "!duplicated(person_id)",
                                          datasources = opals)
 
            dsSwissKnifeClient::dssSubset("o",
                                          "o",
                                          col.filter = paste0("c('person_id')"),
                                          datasources = opals)

            dsSwissKnifeClient::dssDeriveColumn("o",
                                                paste0("tmp_", i), 
                                                "1")

            dsSwissKnifeClient::dssSubset("o",
                                          "o",
                                          col.filter = paste0("c('person_id', 'tmp_", i, "')"),
                                          datasources = opals)

            
            # join into temporary data frame 'tmp'
            dsSwissKnifeClient::dssJoin(c("o", name),
                                        symbol = "tmp",
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            # change NAs into 0s
            dsBaseClient::ds.replaceNA(paste0("tmp$tmp_", i), 
                                       0, 
                                       paste0("tmp2_", i))
            
            dsBaseClient::ds.asFactor(paste0("tmp$tmp2_", i), "tmp2")
            
            dsSwissKnifeClient::dssDeriveColumn("tmp",
                                                paste0("has_", i), 
                                                "tmp2")
            
            dsSwissKnifeClient::dssSubset("tmp",
                                          "tmp",
                                          col.filter = paste0("c('person_id', 'has_", i, "')"),
                                          datasources = opals)
            
            dsSwissKnifeClient::dssJoin(c("tmp", name),
                                        symbol = name,
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            # clean up
            invisible(dsBaseClient::ds.rm("tmp"))
        }
    }

    if (!is.null(age_at_first)) {
        
        if (age_at_first == "visit") {
            
            connection_name <- gsub("\\.", "_", DSI::datashield.resources(opals)[[1]])
            dsResource::dsrAssign("v", "visit_occurrence", connection_name)
            
            # make sure it is ordered by ID and measurement date
            dsSwissKnifeClient::dssSubset("v",
                                          "v",
                                          "order(person_id, visit_start_date)")
            
            dsSwissKnifeClient::dssDeriveColumn("v", 
                                                "visit_start_date_n", 
                                                "as.numeric(as.Date(visit_start_date))")
            
            dsSwissKnifeClient::dssDeriveColumn("v",
                                                "f", 
                                                "'irst_visit_dat.e'")
            
            dsSwissKnifeClient::dssPivot(symbol = "v", 
                                         what = "v", 
                                         value.var = "visit_start_date_n",
                                         formula = "person_id ~ f",
                                         by.col = "person_id",
                                         fun.aggregate = function(x) x[1],
                                         async = TRUE,
                                         datasources = opals)
            
            dsSwissKnifeClient::dssJoin(c("v", name),
                                        symbol = name,
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            dsSwissKnifeClient::dssDeriveColumn(name, 
                                                "age_at_first_visit", 
                                                "round((f.irst_visit_dat.e - as.numeric(as.Date(year_of_birth, origin = '1970-01-01'))) / 365)")
            
            dsSwissKnifeClient::dssSubset(name,
                                          name,
                                          col.filter = "!colnames(baseline) %in% c('f.irst_visit_dat.e', 'year_of_birth', 'birth_datetime')", 
                                          datasources = opals)
            
            invisible(dsBaseClient::ds.rm("v")) 
            
        } else {
            
            where_clause <- paste0("measurement_concept_id in ('", age_at_first, "')")
            
            dsQueryLibrary::dsqLoad(symbol = "ma",
                                    domain = "concept_name",
                                    query_name = "measurement",
                                    where_clause = where_clause,
                                    union = TRUE,
                                    datasources = opals)
            
            dsSwissKnifeClient::dssSubset("ma",
                                          "ma",
                                          "order(person_id, measurement_date)", 
                                          async = FALSE)
            
            dsSwissKnifeClient::dssDeriveColumn("ma", 
                                                "measurement_date_n", 
                                                "as.numeric(as.Date(measurement_date))")
            
            dsSwissKnifeClient::dssDeriveColumn("ma",
                                                "f", 
                                                "'irst_measurement_dat.e'")
            
            dsSwissKnifeClient::dssPivot(symbol = "ma", 
                                         what = "ma", 
                                         value.var = "measurement_date_n",
                                         formula = "person_id ~ f",
                                         by.col = "person_id",
                                         fun.aggregate = function(x) x[1],
                                         async = TRUE,
                                         datasources = opals)
            
            # remove unrealistic values:
            # < 0 is before UNIX time, 1970-01-01
            dsSwissKnifeClient::dssSubset("ma",
                                          "ma",
                                          row.filter = paste0("ma$f.irst_measurement_dat.e > 0"),
                                          datasources = opals)
            
            dsSwissKnifeClient::dssJoin(c("ma", name),
                                        symbol = name,
                                        by = "person_id",
                                        join.type = "full",
                                        datasources = opals)
            
            dsSwissKnifeClient::dssDeriveColumn(name, 
                                                paste0("age_at_first_", age_at_first), 
                                                "round((f.irst_measurement_dat.e - as.numeric(as.Date(year_of_birth, origin = '1970-01-01'))) / 365)")
            
            dsSwissKnifeClient::dssSubset(name,
                                          name,
                                          col.filter = "colnames(baseline) != 'f.irst_measurement_dat.e'",
                                          datasources = opals)
            
            invisible(dsBaseClient::ds.rm("ma"))
            
        }
    }
    
    if (!is.null(age_at_year) | is.null(age_at_first)) {
           
        dsSwissKnifeClient::dssDeriveColumn(name, 
                                            paste0("age_at_year_", age_at_year), 
                                            paste0(age_at_year, " - year_of_birth"))

    }
    
    cat("\n")
}

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
dshSophiaCreateBaseline <- function(procedure_id = NULL) {

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
    
    # --------------------------------------------------------------------------------------------------
    # get year of birth and gender from person table
    # --------------------------------------------------------------------------------------------------
    
    # load person table
    dsQueryLibrary::dsqLoad(symbol = "baseline",
                            domain = "concept_name",
                            query_name = "person",
                            union = TRUE,
                            datasources = opals,
                            async = TRUE)
    
    # keep only columns we need
    dsSwissKnifeClient::dssSubset("baseline",
                                  "baseline",
                                  col.filter = "c('person_id', 'gender', 'year_of_birth')",
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
                            "outcome1", 
                            "1")
            
            dsSwissKnifeClient::dssDeriveColumn("baseline",
                            "outcome0", 
                            "0")
            
            dsSwissKnifeClient::dssJoin(c("p", "baseline"), 
                    symbol = "tmp",
                    by = "person_id",
                    join.type = "full",
                    datasources = opals)
            
            dsBaseClient::ds.Boole(V1 = "tmp$outcome1",
                     V2 = "tmp$outcome0",
                     Boolean.operator = ">",
                     numeric.output = TRUE,
                     na.assign = "0",      
                     newobj = "outcome_bool",
                     datasources = opals)
            
            dsBaseClient::ds.asFactor(input.var.name = "outcome_bool",
                        newobj.name = "outcome_fct",
                        datasources = opals)
            
            dsSwissKnifeClient::dssDeriveColumn("baseline",
                            paste0("has_procedure_", i), 
                            "outcome_fct")
            
            dsSwissKnifeClient::dssSubset("baseline",
                      "baseline",
                      col.filter = '!(colnames(baseline) %in% c("outcome0"))',
                      datasources = opals)
            
        }
    }
    
    dsBaseClient::ds.rm(c("tmp", "p", "outcome_bool", "outcome_fct"))
    
}
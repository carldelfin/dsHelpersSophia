![sophialogo](https://www.imi.europa.eu/sites/default/files/projects/logos/SOPHIA_logo.png)

# dsHelpersSophia

This package provides a series of helper functions intended to make it easier to work with the SOPHIA federated database. The goal is to lower the barrier of entry by (1) having the user install a single package that in turn makes sure that all other required packages are installed, and (2) providing functions for easy access to the federated database and its resources. 

The SOPHIA federated database is built and maintained by [Vital-IT](https://www.sib.swiss/vital-it) at the Swiss Institute of Bioinformatics, and is based around [DataSHIELD](https://www.datashield.org/) and [Opal](https://www.obiba.org/pages/products/opal/). More information about the SOPHIA project itself is available [here](https://www.imi.europa.eu/projects-results/project-factsheets/sophia) and [here](https://imisophia.eu/).

## Installation 

Using [devtools](https://devtools.r-lib.org/): `devtools::install_github("carldelfin/dsHelpersSophia")`

Using [remotes](https://remotes.r-lib.org/): `remotes::install_github("carldelfin/dsHelpersSophia")`

For Windows users, that should be enough. For Linux users, some system packages are required. On a Debian-based system, install these using:

```bash
sudo apt install -y cmake libxml2-dev libcurl4-openssl-dev libssl-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev
```

Adapt accordingly for non-Debian Linux flavours.

Several additional R packages are installed via dependencies. The following are the most important:

* [DSI](https://datashield.github.io/DSI/)
* [dsBaseClient](https://github.com/datashield/dsBaseClient)
* [dsSwissKnifeClient](https://github.com/sib-swiss/dsSwissKnifeClient)
* [dsQueryLibrary](https://github.com/sib-swiss/dsQueryLibrary)

## Usage

* [Login credentials](https://github.com/carldelfin/dsHelpersSophia#login-credentials)
* [dshSophiaConnect](https://github.com/carldelfin/dsHelpersSophia#dshsophiaconnect)
* [dshSophiaLoad](https://github.com/carldelfin/dsHelpersSophia#dshsophiaload)
* [dshSophiaExit](https://github.com/carldelfin/dsHelpersSophia#dshsophiaexit)
* [dshSophiaShow](https://github.com/carldelfin/dsHelpersSophia#dshsophiashow)
* [dshSophiaPrompt](https://github.com/carldelfin/dsHelpersSophia#dshsophiaprompt)
* [dshSophiaOverview](https://github.com/carldelfin/dsHelpersSophia#dshsophiaoverview)
* [dshSophiaMeasureDesc](https://github.com/carldelfin/dsHelpersSophia#dshsophiameasuredesc)

### Login credentials

It is never a good idea to keep sensitive credentials such as login information in R scripts. Although it is possible to enter credentials manually (see `dshSophiaPrompt`), it is usually more convenient to store them in an environment file. Basically, an environment file is list of variables that are automatically read when you start your R session. 

Thus, for a more streamlined experience, most functions in `dsHelpersSophia` will look for user credentials in the `.Renviron` file. If you don't already have one, you can either create it manually (R will look for it in the current user's home directory, which on Linux would be `~/.Renviron` and on Windows `C:\Users\USERNAME\Documents\.Renviron`) or use the [usethis](https://usethis.r-lib.org/) package: `usethis::edit_r_environ()`.

Enter your credentials like this:

```bash
fdb_user="username"
fdb_password="password"
```

### dshSophiaConnect

This function allows the user to connect to the SOPHIA federated database, with the option of including and excluding specific nodes (a node in this context is a server that hosts a database). 

Connecting to the federated database is a two-step process: First, the user connects to each individual *node* in order to retrieve a list of all cohorts that are hosted on that specific node (cohorts in this context refer to a specific dataset associated with a study or research project). Then, the user is disconnected and reconnects to each individual *cohort*. Note that some nodes only host a single cohort.

Importantly, this function does not return anything in the conventional sense, but assigns two objects (`opals` and `nodes_and_cohorts`) to the Global environment using the superassignment operator (`<<-`). These two objects are necessary in order to load and assign database resources and do further work on the federated database. (Note that superassignment in R is generally considered [a bad idea](https://raw-r.org/superassignment.php).)

* Connect to all available nodes, assuming user credentials are available in `.Renviron`:

```R
dshSophiaConnect()
```

* Connect to all available nodes, manually providing credentials:

```R
dshSophiaConnect(username = "username", password = "password")
```

* Connect to specific nodes:

```R
dshSophiaConnect(include = c("node1", "node2"))
```

* Omit specific nodes:

```R
dshSophiaConnect(exclude = c("node1", "node2"))
```

### dshSophiaLoad

This function loads and assigns all database resources into the current R DataSHIELD session. As with `dshSophiaConnect`, this is done on a *per cohort* basis. The two objects assigned by `dshSophiaConnect` (`opals` and `nodes_and_cohorts`) are expected to exist in the Global environment, and if either are missing the user will be prompted to run `dshSophiaConnect` (via `dshSophiaPrompt`).

This function takes no arguments:

```R
dshSophiaLoad()
```

When run successfully, the user can continue to work with the federated database.

### dshSophiaExit

This is a wrapper function that allows the user to disconnect from the federated database. It takes no arguments:

```R
dshSophiaExit()
```

### dshSophiaShow

This function gathers information about available nodes and cohorts and returns this in a data frame. It will look for login credentials in `.Renviron`, but the user may also supply credentials manually.

* Show all nodes and cohorts, assuming username and password is specified in `.Renviron`:

```R
dshSophiaShow()
```

* Show all nodes and cohorts, manually providing username and password:
```R
dshSophiaShow(username = "username", password = "password")
```

### dshSophiaPrompt

This function prompts the user for login details (if those are not available via `Sys.getenv()`) and then connects via `dshSophiaConnect`. The user is also given the option to supply a single character or a list of characters separated by a single space denoting the nodes to either include or exclude. The function is primarily a fallback used within `dshSophiaLoad` when the user has not logged in to the federated system.

### dshSophiaOverview

This function gathers information about the data in each available cohorts and returns this in a data frame. It assumes that the user has connected via `dshSophiaConnect` and loaded database resources via `dshSophiaLoad()`. The function takes no arguments:

```R
overview <- dshSophiaOverview()
```
### dshSophiaMeasureDesc

Test.
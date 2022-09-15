## dsHelpersSophia

![sophialogo](https://www.imi.europa.eu/sites/default/files/projects/logos/SOPHIA_logo.png)

This package provides a series of helper functions intended to make it easier to work with the SOPHIA federated database. In essence, the goal is to lower the barrier of entry by having the user install a single package that in turn makes sure that all other required packages are installed, while also providing functions for easy access to the federated database and its resources. 


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

### Keeping credentials in the .Renviron file

It is never a good idea to keep sensitive credentials such as login information in R scripts. Although it is possible to enter credentials manually (see `dshSophiaPromt`), it is usually more convenient to store them in an environment file. Basically, an environment file is list of variables that are automatically read when you start your R session. 

Thus, for a more streamlined experience, most functions in `dsHelpersSophia` will look for user credentials in the `.Renviron` file. If you don't already have one, you can either create it manually (R will look for it in the current user's home directory, which on Linux would be `~/.Renviron` and on Windows `C:\Users\USERNAME\Documents\.Renviron`) or use the [usethis](https://usethis.r-lib.org/) package: `usethis::edit_r_environ()`.

Enter your credentials like this:

```bash
fdb_user="username"
fdb_password="password"
```

### dshSophiaConnect

This function allows the user to connect to the SOPHIA federated database, with the option of including and excluding specific nodes (a node in this context is a server that hosts a database). 

Connecting to the federated database is a two-step process: First, the user connects to each individual *node* in order to retrieve a list of all cohorts that are hosted on that specific node (cohorts in this context refer to a specific dataset associated with a study or research project). Then, the user is disconnected, and then reconnects to each individual *cohort*. Note that some nodes only host a single cohort.

#### Connect to all nodes, assuming user credentials are available in `.Renviron`:

```R
dshSophiaConnect()
```

#### Connect to all nodes, manually providing credentials:

```R
dshSophiaConnect(username = "username", password = "password")
```

#### Connect to specific nodes:

```R
dshSophiaConnect(include = c("node1", "node2"))
```

#### Omit specific nodes:

```R
dshSophiaConnect(exclude = c("node1", "node2"))
```

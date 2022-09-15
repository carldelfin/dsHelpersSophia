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

Several additional R packages are installed via dependencies. The following three are the most important:

* [dsBaseClient](https://github.com/datashield/dsBaseClient)
* [dsSwissKnifeClient](https://github.com/sib-swiss/dsSwissKnifeClient)
* [dsQueryLibrary](https://github.com/sib-swiss/dsQueryLibrary)

## Use






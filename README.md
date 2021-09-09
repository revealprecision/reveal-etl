# Reveal ETL

## Current Archtecture

The current ETL process is designed to extract data from the OpenSRP database and insert into a Reveal datastore.  This Python based script was built to replace the undocumented and mostly unshared NIFI process.  It is by no means comprehensive and is being expanded on through every implementation.

The script executes within a container that is currently started with a cron job on the Operating System.

## Future Archtecture

The future of Reveal is to become Event Sourced removing the need for an ETL process.  Please refer to <https://revealplatform.atlassian.net/wiki/> for more information about the Product Roadmap.

## Usage

```bash
~/reveal-etl$ ./etl_main.py -h

Check the etl_processor.conf for settings

-h or --help [this menu]
-c or --check [the ability to run SELECT but not INSERT]
-f= or --function= [options: all extract transform load flush]
-e= or --element= [options: plans jurisdictions locations structures settings clients tasks events]
-i= or --interval [amount of hours to pull a delta of data]
```

## Upgrades and improvements needed

* add a lockfile

## Ideas on architecture

* convert the execution from a operating cron into a permanently running container and a cron library for Python scheduler
* add API to trigger functions and elements

## Manual build

```bash
docker build . -t reveal-etl:{BUILD_NUMBER}
```

## CI\CD pipeline

A CI\CD pipeline has been configured to generate the docker container for each PR merged to main.  <https://dev.azure.com/revealprecision/Reveal%20Precision>

## Support

For support please contact Stefanus Heath at Akros <sheath@akros.com>

## Repository

This repository is shared under the T.B.C

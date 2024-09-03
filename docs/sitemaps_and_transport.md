# iSamples Sitemap Generation and Consumption

The iSamples underlying data transport between an iSamples in a Box (iSB) instance and iSamples Central (iSC) is accomplished using sitemaps.  

Each iSB instance writes out sitemaps containing JSON lines files.  iSC consumes the sitemaps, stores them in its things database, and indexes them into its solr index.

## Generation using the export service

The underlying mechanism to write out the json lines files is the [export service](./export_service.md).  

There is a custom export handler that the box instances hit in their cron job, located in [export.py](https://github.com/isamplesorg/isamples_inabox/blob/00236899ffb9185ebbc0625745f8ecbcd41c60c6/isb_web/export.py#L172).

The invocation looks like this:

```
curl "http://localhost:8000/export/create_sitemap" -H "Authorization: Bearer $ORCID_TOKEN_SECRET"
```

The token is a long-lived token, and details about how it is obtained can be found in [authentication_and_identifiers.md](./authentication_and_identifiers.md).

The output is published to a location configured in the iSB instance, the only requirement is that it match the URLs that the sitemap will generate.

## Consumption using the things scripts

iSC is manually programmed to scrape the various iSB sitemaps.  If and when a new provider is added, it will need to be added to the iSC update cron job.  The location where this is all configured is in [isamples_central_update.sh](https://github.com/isamplesorg/isamples_docker/blob/912942ca1d46d8641259c5d653d3e226fc3a9f00/isb/cron/isamples_central_update.sh#L1).  The same python script is responsible for consuming the sitemaps, it is located at [consume_sitemaps.py](https://github.com/isamplesorg/isamples_inabox/blob/00236899ffb9185ebbc0625745f8ecbcd41c60c6/scripts/consume_sitemaps.py#L1).

Because the JSON lines files are already transformed to the core format by the export service, the sitemap things are stored straight into the database in their unaltered format.

After they are stored into the database, they are indexed into solr via the various *things.py (e.g [smithsonian_things.py](https://github.com/isamplesorg/isamples_inabox/blob/00236899ffb9185ebbc0625745f8ecbcd41c60c6/scripts/smithsonian_things.py#L127)) scripts.  Though they are different files, they basically all do the same thing, using the [coreRecordAsSolrDoc](https://github.com/isamplesorg/isamples_inabox/blob/00236899ffb9185ebbc0625745f8ecbcd41c60c6/isb_lib/core.py#L190) function.
